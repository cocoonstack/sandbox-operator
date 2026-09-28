package e2bbuild

import (
	"path"
	"slices"
	"strings"
)

// copyMove unpacks the archive and moves its source entry to the target with Docker COPY semantics, as upstream e2b's copy script does.
const copyMove = `set -o pipefail
mkdir -p "$unpack" && tar -xf "$archive" -C "$unpack" || exit 1
[ -n "$workdir" ] || workdir=$(getent passwd "$user" | cut -d: -f6)
cd "$workdir" || exit 1
case "$target" in /*) ;; *) target="$(pwd)/$target" ;; esac
cd "$(dirname "$source")" || exit 1
entry=$(basename "$source")
if [ ! -e "$entry" ] && [ ! -L "$entry" ]; then echo "source path does not exist: $src"; exit 1; fi
[ -d "$entry" ] || case "$target" in */) mkdir -p "$target" ;; *) mkdir -p "$(dirname "$target")" ;; esac
if [ -L "$entry" ]; then
	chown -h "$owner" "$entry" && mv "$entry" "$target" || exit 1
elif [ -f "$entry" ]; then
	chown "$owner" "$entry" && { [ -z "$mode" ] || chmod "$mode" "$entry"; } && mv "$entry" "$target" || exit 1
elif [ -d "$entry" ]; then
	chown -R "$owner" "$entry" && { [ -z "$mode" ] || chmod -R "$mode" "$entry"; } || exit 1
	if [ ! -e "$target" ]; then
		mkdir -p "$(dirname "${target%/}")" && mv "$entry" "${target%/}" || exit 1
	else
		(cd "$entry" && tar -cf - .) | tar -xf - -C "$target" --keep-directory-symlink --no-overwrite-dir || exit 1
	fi
else
	echo "source is neither a file, a directory nor a symlink: $src"; exit 1
fi
rm -rf "$archive" "$scratch"`

// copyScript is the root shell line that lands a COPY step's upload in the sandbox; the owner defaults to the current user.
func copyScript(state Command, s Step) string {
	owner := state.User + ":" + state.User
	if len(s.Args) > 2 && s.Args[2] != "" {
		owner = s.Args[2]
		if !strings.Contains(owner, ":") {
			owner += ":" + owner
		}
	}
	var mode string
	if len(s.Args) > 3 {
		mode = s.Args[3]
	}
	scratch := "/tmp/" + s.FilesHash
	unpack := scratch + "/unpack"
	vars := [][2]string{
		{"archive", scratch + ".tar"},
		{"scratch", scratch},
		{"unpack", unpack},
		{"src", s.Args[0]},
		{"source", path.Join(unpack, globBase(s.Args[0]))},
		{"target", s.Args[1]},
		{"owner", owner},
		{"mode", mode},
		{"workdir", state.Workdir},
		{"user", state.User},
	}
	var b strings.Builder
	for _, v := range vars {
		b.WriteString(v[0] + "=" + shellQuote(v[1]) + "\n")
	}
	return b.String() + copyMove
}

// globBase is src up to its first segment with a glob; the SDK expands globs into the archive itself.
func globBase(src string) string {
	segs := strings.Split(strings.TrimSuffix(src, "/"), "/")
	if i := slices.IndexFunc(segs, func(seg string) bool { return strings.ContainsAny(seg, "*?[{") }); i >= 0 {
		segs = segs[:i]
	}
	return strings.Join(segs, "/")
}
