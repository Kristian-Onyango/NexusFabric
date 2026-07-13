package node

import (
	"innercore-network/config"
	"innercore-network/types"
)

type Node struct {
	Config *config.Config

	ID   types.NodeID
	Name string

	Capabilities types.Capabilities
}
