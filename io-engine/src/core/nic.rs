use core::default::Default;
use nix::{
    ifaddrs::getifaddrs,
    sys::socket::{IpAddr, Ipv4Addr, Ipv6Addr, SockAddr},
};
use std::{
    collections::BTreeMap,
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
};

/// Formats an option value.
fn fmt_opt<T: ToString>(addr: &Option<T>) -> String {
    match addr {
        Some(ref t) => t.to_string(),
        None => "-".to_string(),
    }
}

/// Controller's internet address configuration: IP address with netmask.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InetConfig<T>
where
    T: Clone + Copy + Display,
{
    /// Network address.
    pub addr: Option<T>,
    /// Network mask.
    pub netmask: Option<T>,
}

impl<T> Default for InetConfig<T>
where
    T: Clone + Copy + Display,
{
    fn default() -> Self {
        Self {
            addr: None,
            netmask: None,
        }
    }
}

impl<T> fmt::Display for InetConfig<T>
where
    T: Clone + Copy + Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", fmt_opt(&self.addr), fmt_opt(&self.netmask))
    }
}

/// NIC's MAC address.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct MacAddr {
    /// MAC address.
    addr: [u8; 6],
}

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.addr[0],
            self.addr[1],
            self.addr[2],
            self.addr[3],
            self.addr[4],
            self.addr[5]
        )
    }
}

impl FromStr for MacAddr {
    type Err = String;
    fn from_str(s: &str) -> Result<MacAddr, Self::Err> {
        match MacAddr::parse(s) {
            Some(mac) => Ok(mac),
            None => Err(format!("Invalid MAC address: '{}'", s)),
        }
    }
}

impl MacAddr {
    /// Creates a new MAC address instance from address bytes.
    pub fn new(addr: [u8; 6]) -> Self {
        Self {
            addr,
        }
    }

    /// Parses MAC address string: six 2-digit hex numbers separated by commas.
    fn parse(s: &str) -> Option<MacAddr> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 6 {
            return None;
        }

        let mut mac = Self::default();

        for (i, p) in parts.iter().enumerate() {
            if p.len() != 2 {
                return None;
            }

            match u8::from_str_radix(p, 16) {
                Ok(x) => mac.addr[i] = x,
                Err(_) => return None,
            }
        }

        Some(mac)
    }
}

/// Describes a network interface controller and its addresses.
#[derive(Clone, Default, Debug, Eq, Hash, PartialEq)]
pub struct Interface {
    /// Name of the network interface.
    pub name: String,
    /// IPv4 network address and netmask of this interface.
    pub inet: InetConfig<Ipv4Addr>,
    /// IPv6 network address and netmask of this interface.
    pub inet6: InetConfig<Ipv6Addr>,
    /// MAC address of this interface.
    pub mac: Option<MacAddr>,
}

impl Interface {
    /// Creates a new Interface instance with the given name.
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// Tests if the interface belongs to the given subnet.
    pub fn ipv4_subnet_eq(&self, net_addr: Ipv4Addr, net_mask: u32) -> bool {
        let (addr, mask) = match (self.inet.addr, self.inet.netmask) {
            (Some(addr), Some(mask)) => (addr, mask),
            _ => return false,
        };

        let mask = u32::from_be(mask.0.s_addr);
        if mask != net_mask {
            return false;
        }

        let addr = u32::from_be(addr.0.s_addr);
        let subnet = addr & mask;
        let subnet = Ipv4Addr::from_std(&std::net::Ipv4Addr::from(subnet));

        subnet == net_addr
    }
}

impl fmt::Display for Interface {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fn fmt_opt<T: ToString>(addr: &Option<T>) -> String {
            match addr {
                Some(ref t) => t.to_string(),
                None => "-".to_string(),
            }
        }

        write!(
            f,
            "{}: inet {} inet6 {} mac {}",
            self.name,
            self.inet,
            self.inet6,
            fmt_opt(&self.mac)
        )
    }
}

/// Lists all network interfaces found on the system.
pub fn find_all_nics() -> Vec<Interface> {
    let mut nics = BTreeMap::<String, Interface>::new();

    for addr in getifaddrs().unwrap() {
        let nic = nics
            .entry(addr.interface_name)
            .or_insert_with_key(|k| Interface::new(k));

        if let Some(sock) = addr.address {
            match sock {
                SockAddr::Inet(inet) => match inet.ip() {
                    IpAddr::V4(v4) => nic.inet.addr = Some(v4),
                    IpAddr::V6(v6) => nic.inet6.addr = Some(v6),
                },
                SockAddr::Link(link) => {
                    nic.mac = Some(MacAddr::new(link.addr()))
                }
                _ => {}
            }
        }

        if let Some(SockAddr::Inet(inet)) = addr.netmask {
            match inet.ip() {
                IpAddr::V4(v4) => nic.inet.netmask = Some(v4),
                IpAddr::V6(v6) => nic.inet6.netmask = Some(v6),
            }
        }
    }

    nics.into_values().into_iter().collect()
}

/// Utility to parse an IPv4 address string into a nix's Ipv4Addr.
pub fn parse_ipv4(addr: &str) -> Result<Ipv4Addr, String> {
    let res = addr
        .parse::<std::net::Ipv4Addr>()
        .map_err(|e| e.to_string())?;
    Ok(Ipv4Addr::from_std(&res))
}

/// Utility to parse an IPv4 subnet string into a nix's Ipv4Addr.
pub fn parse_ipv4_subnet(addr_str: &str) -> Result<(Ipv4Addr, u32), String> {
    let (addr, bits) = match addr_str.split_once('/') {
        Some(p) => p,
        None => return Err(format!("Invalid subnet: '{}'", addr_str)),
    };

    let addr = parse_ipv4(addr)?;
    let addr = u32::from_be(addr.0.s_addr);

    let bits = bits
        .parse::<u32>()
        .map_err(|e| format!("Invalid subnet '{}': {}", addr_str, e))?;

    if bits > 32 {
        return Err(format!("Invalid subnet '{}': suffix too large", addr_str));
    }

    let mask = !0 << (32 - bits);

    let subnet = addr & mask;
    let subnet = Ipv4Addr::from_std(&std::net::Ipv4Addr::from(subnet));
    Ok((subnet, mask))
}
