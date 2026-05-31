// allowedroles/allowed_roles.go
// Allowed roles in the system.
// This is used across multiple layers for policy enforcement.

package allowedroles

// AllowedRoles is the set of valid roles in the mesh network.
// Any role not in this set is automatically treated as "unknown".
var AllowedRoles = map[string]bool{
	"game":    true,
	"chat":    true,
	"cache":   true,
	"storage": true,
	"unknown": true,
}

// IsAllowed returns true if the role is valid
func IsAllowed(role string) bool {
	return AllowedRoles[role]
}
