---
title: Layer 3A — Service Registry
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
file: service.go
---

# <span style="color: #3498db;"> Layer 3A — Service Registry</span>

Layer 3A is the authoritative registry of which nodes are offering which network services (chat, storage, games, etc.). It includes capability tracking, provider scoring, and a background cleanup goroutine.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Architecture Diagram</span>

<table style="width: 100%; border-collapse: collapse; text-align: center;">
  <tr><td style="border: 1px solid #ddd; padding: 8px;">Layer 7: Applications (chat, file sharing, games, etc.)</td</tr>
  <tr><td style="border: 1px solid #ddd; padding: 8px;">⬆️ uses</td</tr>
  <tr><td style="border: 1px solid #ddd; padding: 8px;">
    <code>service/</code> package (Layers 3A + 3B + 3C)
    <table style="width: 100%; margin-top: 5px;">
      <tr>
        <td style="border: 1px solid #ccc;">Layer 3A<br>Service & Provider Registry</td>
        <td style="border: 1px solid #ccc;">Layer 3B<br>Distributed Storage Engine</td>
        <td style="border: 1px solid #ccc;">Layer 3C<br>Metadata Indexing & Search</td>
      </tr>
    </table>
  </td>
  </tr>
  <tr><td style="border: 1px solid #ddd; padding: 8px;">⬆️ uses InnerCore (DHT)</td>
  </tr>
</table>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Core Design Principle</span>

> **No sub-layer does network I/O directly.** Every sub-layer prepares data structures and returns them to the caller. The caller issues the actual DHT operations through InnerCore. This keeps all business logic unit-testable without a running network.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Initialization</span>

Called once at system startup from `integration.go`:

```
service.Init()

// Called after NodeID is known (must be called after Init)
service.SetOwnerID(selfID)
```
```Init()``` sets up the registry and launches the background cleanup goroutine. <br>```SetOwnerID()``` wires the node's identity into the content store and index writer.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Provider Registration</span>

Called automatically by the discovery layer when a DISCOVERY_ANNOUNCE packet is received:

```
result := service.RegisterServicesFromDiscovery(
    deviceID,    // types.NodeID of the announcing device
    services,    // []string{"chat", "presence"}
    servicePort, // int — the port the service runs on
    role,        // string — the node's declared role
)
// result["accepted"] → []string of accepted service names
// result["rejected"] → []string of rejected service names
```
<h3>What happens internally:</h3>

> The device's role is checked against RoleServicePolicy

>Each service is validated against the policy

>Accepted services are registered in ServiceRegistry with a ProviderInfo entry

>A provider score is computed using calculateProviderScore()

>A ProviderTTL timer starts — the entry expires after 45 seconds if not re-announced

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Provider Scoring</span>

Provider selection is score-based, not health-only. Scores are recomputed every time capabilities or health change:

```
func calculateProviderScore(caps types.Capabilities, health float64) float64
```
<table>
    <thead>
        <tr>
            <th>Signal</th>
            <th>Weight</th>
            <th>Reason</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Upload bandwidth</td>
            <td>× 0.4</td>
            <td>Higher bandwidth = better provider</td>
        </tr>
        <tr>
            <td>Available storage</td>
            <td>× 0.003</td>
            <td>More storage = more reliable</td>
        </tr>
        <tr>
            <td>Can store chunks</td>
            <td>+40.0</td>
            <td>Core chunk hosting capability</td>
        </tr>
        <tr>
            <td>Stable node</td>
            <td>+25.0</td>
            <td>Long uptime = reliable source</td>
        </tr>
        <tr>
            <td>Cross-network bridge</td>
            <td>+30.0</td>
            <td>Critical for mesh bridging</td>
        </tr>
        <tr>
            <td>Health</td>
            <td>× 20.0</td>
            <td>Real-time success/failure feedback</td>
        </tr>
    </tbody>
</table>

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Querying Providers</span>

```
// Get all healthy providers for a service, sorted by score (best first)
providers := service.GetServiceProviders("chat", true, 0.5)

// Get full info about a service including its providers
info := service.GetServiceInfo("chat")

// Get all registered services
all := service.GetAllServices()
```
```GetServiceProviders() ```filters by:

Provider announced within the last ```ProviderTTL``` (45 seconds)

Device status "alive" (```if requireAlive = true```)

Device health ≥ ```minHealth```

Results are sorted descending by ```Score```.


<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Background Cleanup</span>

The cleanup goroutine runs every 30 seconds:

```
Every 30s:
  for each service:
    for each provider:
      if time.Since(LastAnnounce) > 45s:
        delete provider
    if no providers left:
      delete service entry
```

This prevents unbounded registry growth as nodes come and go.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Health Feedback</span>

Called by Layer 4 (messaging) after each send attempt:

```
service.UpdateProviderHealth(deviceID, success bool)
```
> On success: health +0.05 (capped at 1.0)

> On failure: health -0.15 (floored at 0.0)

Health changes trigger a score recomputation, so a degrading node automatically sinks in provider rankings.

<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> ProviderInfo Structure</span>

```
type ProviderInfo struct {
    LastAnnounce time.Time          // when last seen
    Metadata     map[string]any     // port, ip, device_name, role
    Health       float64            // 0.0 → 1.0
    Capabilities types.Capabilities // full device capabilities
    Score        float64            // computed fitness score
}
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Constants</span>

<table>
    <thead>
        <tr>
            <th>Constant</th>
            <th>Value</th>
            <th>Meaning</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>ProviderTTL</td>
            <td>45s</td>
            <td>How long a service provider stays registered without re-announcing</td>
        </tr>
    </tbody>
</table>