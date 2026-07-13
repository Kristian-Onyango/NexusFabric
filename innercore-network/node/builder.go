package node

import "innercore-network/config"

func New(cfg *config.Config) *Node {
	return &Node{
		Config: cfg,
	}
}
