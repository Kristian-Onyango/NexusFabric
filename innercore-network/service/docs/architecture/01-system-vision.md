---
title: System Vision & Architecture Principles
last_reviewed: 2026-05-28
valid_for_commit: [YOUR_COMMIT_HASH]
next_review_due: 2026-06-28
parent: ../README.md
---

# <span style="color: #3498db;"> System Vision & Architecture Principles</span>

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">1. Overall System Vision</span>

The system is evolving into:

> **A decentralized hybrid content network** that supports:
> - distributed storage
> - peer-to-peer content sharing
> - chunk-based large file distribution
> - metadata indexing
> - future decentralized search
> - capability-aware node participation
> - streaming-oriented retrieval

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">2. Core Architectural Principle</span>

The system **separates concerns** into independent layers.

>  **This is extremely important.** The system does NOT treat:
> - routing
> - storage
> - indexing
> - search
> - provider discovery
> 
> as the same thing.

Instead:

| Layer | Responsibility |
|-------|----------------|
| Kademlia | Routing & lookup |
| Layer 3A | Service registry & node capabilities |
| Layer 3B | Distributed storage |
| Layer 3C | Metadata indexing & search |
| Discovery Protocol | Peer availability tracking |

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">3. Role of Kademlia</span>

Kademlia is used **ONLY** as:
- routing infrastructure
- distributed key lookup
- distributed key-value transport

Kademlia is **NOT**:
- the storage system itself
- the search engine
- the indexing system

Kademlia only provides: `key → value` lookup

Core operations:
- `FIND_NODE`
- `STORE`
- `FIND_VALUE`

The storage system is implemented **ON TOP** of Kademlia.

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">4. Capability-Aware Participation</span>

Not all nodes are equal. The system uses capability-aware participation.

Nodes may advertise:
- storage capacity
- available storage
- bandwidth
- uptime
- willingness to store chunks

This influences:
- chunk placement
- provider selection
- retrieval prioritization

<hr style="border: 1px solid #ecf0f1;">

## <span style="color: #3498db;">5. Node Role Policies</span>

| Role | Permitted Services |
|------|-------------------|
| `game` | games, chat, matchmaking |
| `chat` | chat, messaging, presence |
| `cache` | cache, storage, cdn |
| `storage` | storage, backup, files |
| `unknown` | (none) |

> ⚠️ Any service registration attempt that violates this policy is rejected.