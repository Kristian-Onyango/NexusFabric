---
title: NexusFabric Documentation
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
---

# <span style="color: #3498db ;">NexusFabric</span> <span style="color: #2c3e50;">— Distributed Storage & Discovery System</span>

A decentralized hybrid content network supporting distributed storage, peer-to-peer content sharing, chunk-based large file distribution, metadata indexing, and capability-aware node participation.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">Architecture Overview</span>

The system combines concepts from Kademlia DHT, BitTorrent, IPFS, and mesh networking systems while remaining customized for unreliable and heterogeneous environments.

> ** Core principle:** The system separates concerns into independent layers. Routing, storage, indexing, search, and provider discovery are NOT treated as the same thing.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">Documentation Map</span>

<table style="width: 100%; border-collapse: collapse; text-align: center;">
  <tr style="background-color: #4c8995;">
    <th style="padding: 10px; border: 1px solid #dddddd;">START HERE (this file)</th>
  </tr>
  <tr>
    <td style="padding: 10px; border: 1px solid #ddd;">
      <code>docs/README.md</code>
    </td>
  </tr>
  <tr>
    <td style="padding: 10px; border: 1px solid #ddd;">
      <table style="width: 100%; margin: 0 auto;">
        <tr>
          <td style="border: none; text-align: center; width: 33%;">⬇️<br><strong>architecture/</strong><br><em>(what & why)</em></td>
          <td style="border: none; text-align: center; width: 33%;">⬇️<br><strong>guides/</strong><br><em>(how to)</em></td>
          <td style="border: none; text-align: center; width: 33%;">⬇️<br><strong>reference/</strong><br><em>(lookup)</em></td>
        </tr>
      </table>
    </td>
  </tr>
</table>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">Quick Links</span>

| I want to... | Read this |
|--------------|-----------|
| Understand the overall system | [`architecture/01-system-vision.md`](architecture/01-system-vision.md) |
| Learn about service registry | [`architecture/02-layer-3A-services.md`](architecture/02-layer-3A-services.md) |
| Understand storage & chunking | [`architecture/03-layer-3B-storage.md`](architecture/03-layer-3B-storage.md) |
| Learn search & indexing | [`architecture/04-layer-3C-search.md`](architecture/04-layer-3C-search.md) |
| See DHT keyspace design | [`architecture/05-keyspace-design.md`](architecture/05-keyspace-design.md) |
| Integrate the system | [`guides/integration.md`](guides/integration.md) |
| See data flow | [`guides/data-flow.md`](guides/data-flow.md) |
| Look up constants | [`reference/constants.md`](reference/constants.md) |
| Check limitations | [`reference/known-limitations.md`](reference/known-limitations.md) |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Layer Reference</span>

| Layer | Responsibility | File |
|-------|----------------|------|
| 3A | Service registry & node capabilities | `service.go` |
| 3B | Distributed storage engine | `types.go`, `content_store.go`, `dht.go`, `provider.go`, `persistence.go`, `retrieval.go` |
| 3C | Metadata indexing & search | `index_types.go`, `tokenizer.go`, `index_writer.go`, `index_reader.go` |
| Keygen | DHT key namespacing | `keygen.go` |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;"> Document Maintenance</span>

- **Review cadence:** Monthly
- **Last review:** 2026-05-28
- **Next review due:** NOT YET SET
- **Stale threshold:** 60 days without update

