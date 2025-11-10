use std::collections::VecDeque;

use crate::{error::Result, HdfsError};
use regex::{Regex, RegexBuilder};

/// A small tuple wrapper holding a pattern string and a character offset.
///
/// The offset is a character index (not a byte index) into the string where
/// parsing should start. This mirrors the Java implementation which tracked
/// character offsets while expanding brace groups.
struct StringWithOffset(String, usize);

// Expand a glob by unwrapping any groups with a '/' so that all
// patterns can be split on slashes to match in individual parts.
/// Expand a glob-like pattern by unwrapping any groups that contain a '/'.
///
/// The expansion ensures that resulting patterns do not contain a '/' inside
/// a top-level brace group. This transformation is useful when matching
/// path components separately after splitting on '/'. The behavior follows
/// the Java `GlobExpander.expand` semantics used in the original project.
pub fn expand_glob(pattern: impl Into<String>) -> Result<Vec<String>> {
    let mut fully_expanded = Vec::<String>::new();
    let mut to_expand = VecDeque::<StringWithOffset>::new();

    to_expand.push_back(StringWithOffset(pattern.into(), 0));
    while let Some(pat) = to_expand.pop_front() {
        let expanded = expand_left_most(&pat)?;
        if expanded.is_empty() {
            fully_expanded.push(pat.0);
        } else {
            // Insert expanded items at front preserving their order so
            // the leftmost alternative is processed first (match Java behavior)
            for val in expanded.into_iter().rev() {
                to_expand.push_front(val);
            }
        }
    }

    Ok(fully_expanded)
}

/// Expand the leftmost outer curly-brace group that contains a '/'.
///
/// This function implements the Java `expandLeftmost` semantics: it splits
/// the first top-level brace group that contains a slash into separate
/// patterns, preserving surrounding prefix and suffix text. Escaped
/// characters (backslash) are respected and copied literally.
fn expand_left_most(string: &StringWithOffset) -> Result<Vec<StringWithOffset>> {
    // Find the leftmost outer curly containing a '/'
    let left_most_byte = match left_most_bracket_with_slash(string)? {
        Some(i) => i,
        None => return Ok(vec![]),
    };

    let pattern = &string.0;
    // prefix is everything before the '{' (byte index)
    let prefix = &pattern[..left_most_byte];

    let mut prefix_str = String::from(prefix);
    let mut suffix = String::new();
    let mut alts: Vec<String> = Vec::new();
    let mut alt = String::new();

    // We'll iterate over characters of the substring starting at left_most_byte
    let mut chars = pattern[left_most_byte..].chars().peekable();
    let mut curly_open: i32 = 0;
    // cur: 0 = before first '{' (prefix), 1 = inside the first-level alt, 2 = suffix
    let mut cur: u8 = 0;

    while let Some(c) = chars.next() {
        if cur == 2 {
            // already in suffix, copy everything
            suffix.push(c);
            continue;
        }

        if c == '\\' {
            // escaped character: consume next char and append
            match chars.next() {
                Some(next) => {
                    if cur == 1 {
                        alt.push(next);
                    } else {
                        // if still before the first-level '{', append into prefix
                        prefix_str.push(next);
                    }
                }
                None => {
                    return Err(HdfsError::InvalidPath(format!(
                        "Illegal file pattern: An escaped character is not present for glob {}",
                        pattern
                    )));
                }
            }
            continue;
        }

        if c == '{' {
            if curly_open == 0 {
                // opening the first-level group
                alt.clear();
                cur = 1;
            } else if cur == 1 {
                alt.push(c);
            } else {
                prefix_str.push(c);
            }
            curly_open += 1;
        } else if c == '}' && curly_open > 0 {
            curly_open -= 1;
            if curly_open == 0 {
                // end of the first-level group
                alts.push(alt.clone());
                alt.clear();
                cur = 2; // switch to suffix
            } else if cur == 1 {
                alt.push(c);
            } else {
                prefix_str.push(c);
            }
        } else if c == ',' {
            if curly_open == 1 && cur == 1 {
                // separator between alternatives at top level of this group
                alts.push(alt.clone());
                alt.clear();
            } else if cur == 1 {
                alt.push(c);
            } else {
                prefix_str.push(c);
            }
        } else {
            // regular character
            if cur == 1 {
                alt.push(c);
            } else {
                prefix_str.push(c);
            }
        }
    }

    let mut res = Vec::new();
    let prefix_char_count = prefix_str.chars().count();
    for s in alts.into_iter() {
        let new_pattern = format!("{}{}{}", prefix_str, s, suffix);
        res.push(StringWithOffset(new_pattern, prefix_char_count));
    }

    Ok(res)
}

/// Find the index (character index) of the leftmost outer opening brace
/// that contains a '/' before its matching closing brace.
///
/// Returns `Ok(Some(idx))` where `idx` is the byte index in the string of
/// the opening '{' if found, or `Ok(None)` if no such top-level brace group
/// contains a slash. Escaped characters are honored and do not count as
/// regular characters.
fn left_most_bracket_with_slash(string: &StringWithOffset) -> Result<Option<usize>> {
    let mut curly_open = 0;
    let mut left_most = 0;
    let mut seen_slash = false;

    let StringWithOffset(pattern, offset) = string;
    let mut chars = pattern.char_indices().skip(*offset);
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => {
                if chars.next().is_none() {
                    return Err(HdfsError::InvalidPath(format!(
                        "Escape character cannot end a path: {}",
                        pattern
                    )));
                }
            }
            '{' => {
                if curly_open == 0 {
                    left_most = i;
                }
                curly_open += 1;
            }
            '}' if curly_open > 0 => {
                curly_open -= 1;
                if curly_open == 0 && seen_slash {
                    return Ok(Some(left_most));
                }
            }
            '/' if curly_open > 0 => seen_slash = true,
            _ => (),
        }
    }

    Ok(None)
}

pub fn get_path_components(pattern: &str) -> Vec<&str> {
    pattern.split('/').filter(|c| !c.is_empty()).collect()
}

/// Unescape simple backslash escapes in a path component.
pub fn unescape_component(comp: &str) -> String {
    let mut unescaped = String::new();
    let mut chars = comp.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(n) = chars.next() {
                unescaped.push(n);
            }
        } else {
            unescaped.push(c);
        }
    }

    unescaped
}

/// A compiled glob pattern helper.
///
/// This mirrors the Java `GlobPattern` class converted to Rust. It
/// compiles a glob-like pattern into a `regex::Regex` and tracks whether
/// the pattern contained any wildcard constructs.
pub struct GlobPattern {
    compiled: Regex,
    has_wildcard: bool,
}

impl GlobPattern {
    /// Compile a glob pattern into a `GlobPattern`.
    pub fn new(glob: &str) -> Result<Self> {
        let mut regex = String::with_capacity(glob.len() * 2);
        let mut set_open: i32 = 0;
        let mut curly_open: i32 = 0;
        let mut has_wildcard = false;

        let mut chars = glob.chars().peekable();
        // track previous character for context (used for [! -> [^)
        let mut prev: Option<char> = None;
        let mut char_index: usize = 0; // character index for errors

        while let Some(c) = chars.next() {
            match c {
                '\\' => {
                    // escape: copy backslash and next char literally
                    let next = chars.next().ok_or_else(|| {
                        HdfsError::InvalidPath(format!(
                            "Missing escaped character at pos {}",
                            char_index + 1
                        ))
                    })?;
                    regex.push('\\');
                    regex.push(next);
                    char_index += 1; // we consumed one extra char
                    prev = Some(next);
                    char_index += 1;
                    continue;
                }
                '.' | '$' | '(' | ')' | '|' | '+' => {
                    // escape regex special chars that are not glob specials
                    regex.push('\\');
                }
                '*' => {
                    // glob * -> regex .*
                    regex.push_str(".*");
                    has_wildcard = true;
                    prev = Some('*');
                    char_index += 1;
                    continue;
                }
                '?' => {
                    // glob ? -> regex .
                    regex.push('.');
                    has_wildcard = true;
                    prev = Some('?');
                    char_index += 1;
                    continue;
                }
                '{' => {
                    // start non-capturing group
                    regex.push_str("(?:");
                    curly_open += 1;
                    has_wildcard = true;
                    prev = Some('{');
                    char_index += 1;
                    continue;
                }
                ',' => {
                    if curly_open > 0 {
                        regex.push('|');
                    } else {
                        regex.push(',');
                    }
                    prev = Some(',');
                    char_index += 1;
                    continue;
                }
                '}' => {
                    if curly_open > 0 {
                        curly_open -= 1;
                        regex.push(')');
                        prev = Some('}');
                        char_index += 1;
                        continue;
                    }
                }
                '[' => {
                    if set_open > 0 {
                        return Err(HdfsError::InvalidPath(format!(
                            "Unclosed character class at pos {}",
                            char_index
                        )));
                    }
                    set_open += 1;
                    has_wildcard = true;
                }
                '^' => {
                    if set_open == 0 {
                        regex.push('\\');
                    }
                }
                '!' => {
                    // [! -> [^ inside character class
                    if set_open > 0 && prev == Some('[') {
                        regex.push('^');
                    } else {
                        regex.push('!');
                    }
                    prev = Some('!');
                    char_index += 1;
                    continue;
                }
                ']' => {
                    // close set
                    set_open = 0;
                }
                _ => {}
            }

            regex.push(c);
            prev = Some(c);
            char_index += 1;
        }

        if set_open > 0 {
            return Err(HdfsError::InvalidPath(format!(
                "Unclosed character class at pos {}",
                glob.chars().count()
            )));
        }
        if curly_open > 0 {
            return Err(HdfsError::InvalidPath(format!(
                "Unclosed group at pos {}",
                glob.chars().count()
            )));
        }

        // Anchor the regex so it matches the entire input (Java Pattern.matches
        // requires a full match). Use DOTALL so '.' matches newline like
        // Java's Pattern.DOTALL.
        let anchored = format!("^{}$", regex);
        let re = RegexBuilder::new(&anchored)
            .dot_matches_new_line(true)
            .build()
            .map_err(|e| HdfsError::InvalidPath(format!("Regex compile error: {}", e)))?;

        Ok(GlobPattern {
            compiled: re,
            has_wildcard,
        })
    }

    /// Test whether an input matches the compiled glob.
    pub fn matches(&self, s: &str) -> bool {
        self.compiled.is_match(s)
    }

    /// Whether the glob contained wildcard constructs.
    pub fn has_wildcard(&self) -> bool {
        self.has_wildcard
    }
}

#[cfg(test)]
mod test {
    use crate::glob::{expand_glob, left_most_bracket_with_slash, GlobPattern, StringWithOffset};

    #[test]
    fn test_expand_glob() {
        assert_eq!(expand_glob("{a/b}").unwrap(), vec!["a/b".to_string()]);
        assert_eq!(
            expand_glob("{a/b,c/d}").unwrap(),
            vec!["a/b".to_string(), "c/d".to_string()]
        );
        // Examples from the Java doc
        assert_eq!(expand_glob("/}{a/b}").unwrap(), vec!["/}a/b".to_string()]);
        assert_eq!(
            expand_glob("p{a/b,c/d}s").unwrap(),
            vec!["pa/bs".to_string(), "pc/ds".to_string()]
        );
        assert_eq!(
            expand_glob("{a/b,c/d,{e,f}}").unwrap(),
            vec!["a/b".to_string(), "c/d".to_string(), "{e,f}".to_string()]
        );
        assert_eq!(
            expand_glob("{a,b}/{b,{c/d,e/f}}").unwrap(),
            vec![
                "{a,b}/b".to_string(),
                "{a,b}/c/d".to_string(),
                "{a,b}/e/f".to_string()
            ]
        );
        // Escaped character: c/\d -> c/d
        assert_eq!(
            expand_glob("{a,b}/{c/\\d}").unwrap(),
            vec!["{a,b}/c/d".to_string()]
        );
    }

    #[test]
    fn test_left_most_bracket_with_slash() {
        assert_eq!(
            left_most_bracket_with_slash(&StringWithOffset(
                "/path/{to/nested,{file,other}}".to_string(),
                0
            ))
            .unwrap(),
            Some(6)
        );

        assert_eq!(
            left_most_bracket_with_slash(&StringWithOffset(
                "/path/{to,{file,other}}".to_string(),
                0
            ))
            .unwrap(),
            None
        );

        assert_eq!(
            left_most_bracket_with_slash(&StringWithOffset(
                "/path/{to,{file/other}}".to_string(),
                0
            ))
            .unwrap(),
            Some(6)
        );
    }

    #[test]
    fn test_globpattern_all() {
        // basic star and question
        let g = GlobPattern::new("*.txt").unwrap();
        assert!(g.has_wildcard());
        assert!(g.matches("file.txt"));
        assert!(!g.matches("file.jpg"));

        let g2 = GlobPattern::new("data?.csv").unwrap();
        assert!(g2.matches("data1.csv"));
        assert!(!g2.matches("data12.csv"));

        // brace groups
        let g3 = GlobPattern::new("a{b,c}d").unwrap();
        assert!(g3.matches("abd"));
        assert!(g3.matches("acd"));
        assert!(!g3.matches("abcd"));

        // error cases
        assert!(GlobPattern::new("file[").is_err());
        assert!(GlobPattern::new("a{b").is_err());

        // character classes and ranges
        let g4 = GlobPattern::new("[abc].txt").unwrap();
        assert!(g4.matches("a.txt"));
        assert!(g4.matches("b.txt"));
        assert!(!g4.matches("d.txt"));

        let g5 = GlobPattern::new("[!a].txt").unwrap();
        assert!(g5.matches("b.txt"));
        assert!(!g5.matches("a.txt"));

        let g6 = GlobPattern::new("file[0-9].txt").unwrap();
        assert!(g6.matches("file1.txt"));
        assert!(!g6.matches("filex.txt"));

        // star and question behavior
        let g7 = GlobPattern::new("a*b").unwrap();
        assert!(g7.matches("ab"));
        assert!(g7.matches("axxb"));
        assert!(!g7.matches("ac"));

        let g8 = GlobPattern::new("?end").unwrap();
        assert!(g8.matches("aend"));
        assert!(!g8.matches("xxend"));

        // escapes and literal punctuation
        let g9 = GlobPattern::new("file\\.txt").unwrap();
        assert!(g9.matches("file.txt"));

        let g10 = GlobPattern::new("bang!").unwrap();
        assert!(g10.matches("bang!"));

        // trailing backslash without escaped char is an error
        assert!(GlobPattern::new("abc\\").is_err());
    }
}
