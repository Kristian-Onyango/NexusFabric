package allowedroles
//changed this in integration.go
discoveryPort := 37020
messagePort := 51000 + instanceID*10
//to this
discoveryPort := 37020 + instanceID
messagePort := 51000 + instanceID*10
//is this okay i believe real devices different machine will use 37020 but same machine willhave different ports

//changed these in discovery.go 

//from
Payload: mustMarshal(map[string]any{"services": services, "service_port": servicePort}),
//to
Payload: mustMarshal(map[string]any{
    "services":      services,
    "service_port":  servicePort,
    "message_port":  LocalMessagePort,   // ← the port this node is actually listening on for Layer 4
}),

//from
UpdateNetworkCallback func(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int, )
//to
UpdateNetworkCallback func(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int, messagePort int)

//from
UpdateNetworkCallback(p.Header.SourceNodeID, senderIP, "", p.Capabilities, services, servicePort)
//to
UpdateNetworkCallback(p.Header.SourceNodeID, senderIP, "", p.Capabilities, services, servicePort, messagePort)

//added this after After the existing servicePort extraction
messagePort := 51000 // fallback
if mp, ok := payloadMap["message_port"].(float64); ok {
    messagePort = int(mp)
}

//In network/network.go
//added this in PeerEntry struct
MessagePort  int          // ← NEW
//updated the function
func UpdateNode(nodeID types.NodeID, ip string, name string, caps types.Capabilities, services []string, servicePort int, messagePort int) //to accept hte extra port
//Inside the if !exists block:
entry = &PeerEntry{
    // ... existing fields ...
    ServicePort:  servicePort,
    MessagePort:  messagePort,   // ← NEW
//And in the update path (when the peer already exists):
if messagePort > 0 {
    entry.MessagePort = messagePort
}

//in message.go changed
//from 
addr := &net.UDPAddr{IP: net.ParseIP(peer.IP), Port: discovery.LocalMessagePort}
//to
port := peer.MessagePort
if port == 0 {
    port = discovery.LocalMessagePort // temporary fallback while table is warming up
}
addr := &net.UDPAddr{IP: net.ParseIP(peer.IP), Port: port}

//in discovery/nodeid.go changed
//from
instanceSuffix := ""
if len(os.Args) > 1 && os.Args[1] == "1" {
    instanceSuffix = "_instance1"
}
//to
instanceSuffix := ""
if len(os.Args) > 1 {
    instanceSuffix = "_instance" + os.Args[1]
}
//in message.go changed
//from
network.UpdateNode(senderID, remoteIP, "", types.Capabilities{}, nil, 5000, 51000)
//to
// Prefer the remote UDP source port if available, otherwise fall back
msgPort := addr.Port
if msgPort == 0 {
    msgPort = 51000
}
network.UpdateNode(senderID, remoteIP, "", types.Capabilities{}, nil, 5000, msgPort)
