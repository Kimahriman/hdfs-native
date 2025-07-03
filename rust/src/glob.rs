use std::collections::VecDeque;

use crate::{error::Result, HdfsError};

struct StringWithOffset(String, usize);

// Expand a glob by unwrapping any groups with a '/' so that all
// patterns can be split on slashes to match in individual parts.
fn expand_glob(pattern: impl Into<String>) -> Result<Vec<String>> {
    let mut fully_expanded = Vec::<String>::new();
    let mut to_expand = VecDeque::<StringWithOffset>::new();

    to_expand.push_back(StringWithOffset(pattern.into(), 0));
    while let Some(pat) = to_expand.pop_front() {
        let expanded = expand_left_most(&pat)?;
        if expanded.is_empty() {
            fully_expanded.push(pat.0);
        } else {
            for val in expanded.into_iter() {
                to_expand.push_front(val);
            }
        }
    }

    Ok(fully_expanded)
}

fn expand_left_most(string: &StringWithOffset) -> Result<Vec<StringWithOffset>> {
    if let Some(left_most) = left_most_bracket_with_slash(string)? {
        Ok(vec![])
    } else {
        Ok(vec![])
    }
}

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

#[cfg(test)]
mod test {
    use crate::glob::{expand_glob, left_most_bracket_with_slash, StringWithOffset};

    #[test]
    fn test_expand_glob() {
        assert_eq!(expand_glob("{a/b}").unwrap(), vec!["a/b".to_string()]);
        assert_eq!(
            expand_glob("{a/b,c/d}").unwrap(),
            vec!["a/b".to_string(), "c/d".to_string()]
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
}
