package e2bcompat

import (
	"strings"
	"unicode/utf8"
)

// PublicID renders a node-local claim id as a DNS-label-safe sandbox id, the
// form handed to e2b clients. A claim id is "sb_" + hex, and the SDK derives
// the envd host as "{port}-{sandboxID}.{domain}", so the underscore would make
// a created sandbox unreachable.
func PublicID(claimID string) string {
	if !needsRewrite(claimID) {
		return claimID
	}
	return strings.Map(func(r rune) rune {
		if isDNSSafe(r) {
			return r
		}
		return '-'
	}, strings.ToLower(claimID))
}

// MatchesID reports whether a live sandbox's claim id is the one a client asked
// for, accepting both the raw claim id and its published DNS-safe rendering.
// The rendering is compared in place: the store sweep calls this per scanned
// entry, and building PublicID per candidate would allocate O(fleet) per lookup.
func MatchesID(claimID, requested string) bool {
	if claimID == "" || requested == "" {
		return false
	}
	if claimID == requested {
		return true
	}
	if !isASCII(claimID) {
		return PublicID(claimID) == requested
	}
	if len(claimID) != len(requested) {
		return false
	}
	for i := range len(claimID) {
		c := claimID[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if !isDNSSafe(rune(c)) {
			c = '-'
		}
		if requested[i] != c {
			return false
		}
	}
	return true
}

func needsRewrite(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool { return !isDNSSafe(r) })
}

func isASCII(s string) bool {
	return !strings.ContainsFunc(s, func(r rune) bool { return r >= utf8.RuneSelf })
}

// isDNSSafe reports whether r is legal inside a DNS label (RFC 1123): lowercase
// alphanumerics and the hyphen.
func isDNSSafe(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-'
}
