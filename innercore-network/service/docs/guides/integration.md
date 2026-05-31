
```markdown
---
title: Integration Guide
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
---
```

# <span style="color: #3498db;"> Integration Guide</span>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Startup Sequence</span>

```go
// In integration.go Init():

// 1. Initialize storage (Layer 5)
storage.InitializeStorage()

// 2. Initialize discovery (Layer 1)
discovery.Init(services, port, discoveryPort, messagePort)

// 3. Initialize InnerCore (Layer 1.5)
ic := innercore.New(storage.GlobalStorage.GetEngine(), message.SendPacket)
ic.SetNodeID(discovery.GetMyNodeID())

// 4. Initialize service layer — Layer 3A
service.Init()
service.SetOwnerID(discovery.GetMyNodeID())

// 5. Initialize DHT publisher — Layer 3B
service.InitDHTPublisher(ic)

// 6. Initialize search index — Layer 3C
service.InitIndex()

// 7. Start background loops
go service.StartProviderCleanup()
go service.GetIndexWriter().StartMaintenanceLoop(
    service.GetContentStore(),
    func(records []service.IndexWriteRecord) {
        for _, r := range records {
            ic.SendStore(r.DHTKey, r.Entry)
        }
    },
)
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Publishing a File</span>

```go
// Simple path — one call handles everything
result, err := service.PublishContent(
    "Suits.S01E01.mkv",
    "video",
    fileBytes,
    []string{"tv", "drama"},
    []string{"suits", "harvey", "legal"},
)

if err != nil {
    log.Printf("publish failed: %v", err)
    return
}

fmt.Printf("Published: %s\n", result.Meta.ContentID)
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Searching for Content</span>

```go
// Phase 1: instant local cache search
query := service.SearchQuery{
    RawQuery:   "suits season 1",
    TypeFilter: "video",
    MaxResults: 20,
    Page:       0,
}

reader := service.GetIndexReader()
resp := reader.SearchLocal(query)

if resp.TotalFound == 0 {
    // Phase 2: fetch from DHT
    requests := reader.GetDHTKeysForQuery(query)
    for _, req := range requests {
        // ic.FindValue(req.DHTKey) → get bucket → FeedBucket
        reader.FeedBucket(req.Keyword, fetchedBucket)
    }
    resp = reader.SearchLocal(query)
}

for _, result := range resp.Results {
    fmt.Printf("[%.2f] %s (%s)\n", result.Score, result.Name, result.Type)
}
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Retrieving Content by ID</span>

```go
result, err := service.RetrieveContent(contentID)
if err != nil {
    log.Printf("not found: %v", err)
    return
}

if result.Data != nil {
    // Small file — full bytes available
    os.WriteFile(result.Meta.Name, result.Data, 0644)
} else {
    // Large file — use result.Chunks map
    // Chunk reassembly (future work)
}
```
<hr style="border: 1px solid #ecf0f1;">
<span style="color: #3498db;"> Background Maintenance</span>

The following goroutines should be started at system init:

<table>
    <thead>
        <tr>
            <th>Goroutine</th>
            <th>Frequency</th>
            <th>Purpose</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>StartProviderCleanup()</td>
            <td>Every 2 min</td>
            <td>Removes expired provider announcements</td>
        </tr>
        <tr>
            <td>StartMaintenanceLoop()</td>
            <td>Every 20 min</td>
            <td>Re-announces index entries to DHT</td>
        </tr>
        <tr>
            <td>PurgeExpiredCache() (manual)</td>
            <td>Every 30 min</td>
            <td>Cleans stale search cache entries</td>
        </tr>
    </tbody>
</table>