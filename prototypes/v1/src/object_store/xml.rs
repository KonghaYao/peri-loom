use anyhow::{bail, ensure, Result};

pub(super) fn values(document: &str, tag: &str) -> Result<Vec<String>> {
    let opening = format!("<{tag}>");
    let closing = format!("</{tag}>");
    let mut rest = document;
    let mut values = Vec::new();
    while let Some(start) = rest.find(&opening) {
        rest = &rest[start + opening.len()..];
        let end = rest
            .find(&closing)
            .ok_or_else(|| anyhow::anyhow!("malformed S3 XML"))?;
        ensure!(!rest[..end].contains('<'), "unexpected nested S3 XML");
        values.push(unescape(&rest[..end])?);
        rest = &rest[end + closing.len()..];
    }
    Ok(values)
}

fn unescape(value: &str) -> Result<String> {
    let mut result = String::new();
    let mut rest = value;
    while let Some(start) = rest.find('&') {
        result.push_str(&rest[..start]);
        rest = &rest[start + 1..];
        let end = rest
            .find(';')
            .ok_or_else(|| anyhow::anyhow!("invalid XML entity"))?;
        let entity = &rest[..end];
        result.push(match entity {
            "amp" => '&',
            "lt" => '<',
            "gt" => '>',
            "quot" => '"',
            "apos" => '\'',
            _ => {
                let number = if let Some(hex) = entity.strip_prefix("#x") {
                    u32::from_str_radix(hex, 16)?
                } else if let Some(decimal) = entity.strip_prefix('#') {
                    decimal.parse()?
                } else {
                    bail!("unsupported XML entity");
                };
                char::from_u32(number).ok_or_else(|| anyhow::anyhow!("invalid XML codepoint"))?
            }
        });
        rest = &rest[end + 1..];
    }
    result.push_str(rest);
    Ok(result)
}
