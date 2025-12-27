use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::super::IrEvaluator;

impl<'a> IrEvaluator<'a> {
    pub(crate) fn fn_net_ip_from_string(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                use std::net::IpAddr;
                match s.parse::<IpAddr>() {
                    Ok(IpAddr::V4(ipv4)) => Ok(Value::Bytes(ipv4.octets().to_vec())),
                    Ok(IpAddr::V6(ipv6)) => Ok(Value::Bytes(ipv6.octets().to_vec())),
                    Err(_) => Err(Error::InvalidQuery(format!("Invalid IP address: {}", s))),
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_FROM_STRING expects a string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_net_safe_ip_from_string(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                use std::net::IpAddr;
                match s.parse::<IpAddr>() {
                    Ok(IpAddr::V4(ipv4)) => Ok(Value::Bytes(ipv4.octets().to_vec())),
                    Ok(IpAddr::V6(ipv6)) => Ok(Value::Bytes(ipv6.octets().to_vec())),
                    Err(_) => Ok(Value::Null),
                }
            }
            _ => Ok(Value::Null),
        }
    }

    pub(crate) fn fn_net_ip_to_string(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(bytes) => {
                use std::net::{Ipv4Addr, Ipv6Addr};
                if bytes.len() == 4 {
                    let arr: [u8; 4] = bytes[..4].try_into().unwrap();
                    Ok(Value::String(Ipv4Addr::from(arr).to_string()))
                } else if bytes.len() == 16 {
                    let arr: [u8; 16] = bytes[..16].try_into().unwrap();
                    Ok(Value::String(Ipv6Addr::from(arr).to_string()))
                } else {
                    Err(Error::InvalidQuery(
                        "NET.IP_TO_STRING expects 4 or 16 bytes".into(),
                    ))
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_TO_STRING expects a bytes argument".into(),
            )),
        }
    }

    pub(crate) fn fn_net_ipv4_from_int64(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => {
                let bytes = (*n as u32).to_be_bytes();
                Ok(Value::Bytes(bytes.to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IPV4_FROM_INT64 expects an integer argument".into(),
            )),
        }
    }

    pub(crate) fn fn_net_ipv4_to_int64(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(bytes) if bytes.len() == 4 => {
                let arr: [u8; 4] = bytes[..4].try_into().unwrap();
                let n = u32::from_be_bytes(arr);
                Ok(Value::Int64(n as i64))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IPV4_TO_INT64 expects 4 bytes".into(),
            )),
        }
    }

    pub(crate) fn fn_net_host(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(url) => {
                if let Some(host_start) = url.find("://").map(|i| i + 3) {
                    let rest = &url[host_start..];
                    let host_end = rest
                        .find('/')
                        .or_else(|| rest.find('?'))
                        .or_else(|| rest.find('#'))
                        .unwrap_or(rest.len());
                    let host_port = &rest[..host_end];
                    let host = host_port.split(':').next().unwrap_or(host_port);
                    Ok(Value::String(host.to_lowercase()))
                } else {
                    let host = url.split('/').next().unwrap_or(url);
                    let host = host.split(':').next().unwrap_or(host);
                    Ok(Value::String(host.to_lowercase()))
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.HOST expects a string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_net_public_suffix(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(_) => {
                let host_result = self.fn_net_host(args)?;
                if let Value::String(host) = host_result {
                    let parts: Vec<&str> = host.split('.').collect();
                    if parts.len() >= 2 {
                        Ok(Value::String(parts[parts.len() - 1].to_string()))
                    } else if parts.len() == 1 {
                        Ok(Value::String(parts[0].to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.PUBLIC_SUFFIX expects a string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_net_reg_domain(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(_) => {
                let host_result = self.fn_net_host(args)?;
                if let Value::String(host) = host_result {
                    let parts: Vec<&str> = host.split('.').collect();
                    if parts.len() >= 2 {
                        Ok(Value::String(format!(
                            "{}.{}",
                            parts[parts.len() - 2],
                            parts[parts.len() - 1]
                        )))
                    } else {
                        Ok(Value::String(host))
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.REG_DOMAIN expects a string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_net_ip_in_net(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "NET.IP_IN_NET requires IP and network arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Bytes(ip_bytes), Value::String(network)) => {
                let parts: Vec<&str> = network.split('/').collect();
                if parts.len() != 2 {
                    return Ok(Value::Bool(false));
                }
                let prefix_len: u8 = match parts[1].parse() {
                    Ok(p) => p,
                    Err(_) => return Ok(Value::Bool(false)),
                };
                let network_octets: Vec<u8> =
                    parts[0].split('.').filter_map(|s| s.parse().ok()).collect();
                if network_octets.len() != 4 || ip_bytes.len() != 4 {
                    return Ok(Value::Bool(false));
                }
                let full_bytes = (prefix_len / 8) as usize;
                let remaining_bits = prefix_len % 8;
                for i in 0..full_bytes {
                    if ip_bytes[i] != network_octets[i] {
                        return Ok(Value::Bool(false));
                    }
                }
                if remaining_bits > 0 && full_bytes < 4 {
                    let mask = !((1u8 << (8 - remaining_bits)) - 1);
                    if (ip_bytes[full_bytes] & mask) != (network_octets[full_bytes] & mask) {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_IN_NET expects bytes and string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_net_ip_is_private(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(bytes) if bytes.len() == 4 => {
                let is_private = (bytes[0] == 10)
                    || (bytes[0] == 172 && (16..=31).contains(&bytes[1]))
                    || (bytes[0] == 192 && bytes[1] == 168);
                Ok(Value::Bool(is_private))
            }
            Value::Bytes(_) => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery(
                "NET.IP_IS_PRIVATE expects bytes argument".into(),
            )),
        }
    }

    pub(crate) fn fn_net_ip_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "NET.IP_TRUNC requires IP and prefix length arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Bytes(bytes), Value::Int64(prefix_len)) => {
                let mut result = bytes.clone();
                let full_bytes = (*prefix_len / 8) as usize;
                let remaining_bits = (*prefix_len % 8) as u8;

                for (i, byte) in result.iter_mut().enumerate() {
                    if i < full_bytes {
                        continue;
                    } else if i == full_bytes && remaining_bits > 0 {
                        let mask = !((1u8 << (8 - remaining_bits)) - 1);
                        *byte &= mask;
                    } else {
                        *byte = 0;
                    }
                }
                Ok(Value::Bytes(result))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_TRUNC expects bytes and integer arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_net_ip_net_mask(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "NET.IP_NET_MASK requires num_bytes and prefix_length arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Int64(num_bytes), Value::Int64(prefix_len)) => {
                let mut mask = vec![0u8; *num_bytes as usize];
                let full_bytes = (*prefix_len / 8) as usize;
                let remaining_bits = (*prefix_len % 8) as u8;

                for (i, byte) in mask.iter_mut().enumerate() {
                    if i < full_bytes {
                        *byte = 0xFF;
                    } else if i == full_bytes && remaining_bits > 0 {
                        *byte = !((1u8 << (8 - remaining_bits)) - 1);
                    }
                }
                Ok(Value::Bytes(mask))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_NET_MASK expects integer arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_net_make_net(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "NET.MAKE_NET requires IP and prefix length arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Bytes(bytes), Value::Int64(prefix_len)) => {
                let ip_str = if bytes.len() == 4 {
                    format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3])
                } else if bytes.len() == 16 {
                    let parts: Vec<String> = bytes
                        .chunks(2)
                        .map(|chunk| format!("{:x}{:02x}", chunk[0], chunk[1]))
                        .collect();
                    parts.join(":")
                } else {
                    return Err(Error::InvalidQuery(
                        "NET.MAKE_NET expects 4 or 16 byte IP address".into(),
                    ));
                };
                Ok(Value::String(format!("{}/{}", ip_str, prefix_len)))
            }
            _ => Err(Error::InvalidQuery(
                "NET.MAKE_NET expects bytes and integer arguments".into(),
            )),
        }
    }
}
