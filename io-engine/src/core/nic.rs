use core::default::Default;
use nix::ifaddrs::getifaddrs;

use std::{
    fmt,
    fmt::{Debug, Display, Formatter},
    net::{Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

/// An IpAddr wrapper which provides handy convertion methods as well as starting to
/// support exposing the scope id out of the ipv6 address.
#[derive(Copy, Clone, Debug)]
pub enum SIpAddr {
    V4(Ipv4Addr),
    V6(Ipv6Addr, u32),
}
impl Display for SIpAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SIpAddr::V4(ip) => write!(f, "{ip}"),
            SIpAddr::V6(ip, scope_id) if scope_id != &0 => {
                write!(f, "{ip}%{scope_id}")
            }
            SIpAddr::V6(ip, _) => write!(f, "{ip}"),
        }
    }
}

impl SIpAddr {
    /// Get the [`std::net::IpAddr`] of `Self`.
    pub(crate) fn ip(&self) -> std::net::IpAddr {
        match self {
            SIpAddr::V4(ip) => (*ip).into(),
            SIpAddr::V6(ip, _) => (*ip).into(),
        }
    }
}

impl From<std::net::SocketAddr> for SIpAddr {
    fn from(value: std::net::SocketAddr) -> Self {
        match value {
            std::net::SocketAddr::V4(socket) => socket.into(),
            std::net::SocketAddr::V6(socket) => socket.into(),
        }
    }
}
impl From<std::net::SocketAddrV4> for SIpAddr {
    fn from(socket: std::net::SocketAddrV4) -> Self {
        Self::V4(*socket.ip())
    }
}
impl From<std::net::SocketAddrV6> for SIpAddr {
    fn from(socket: std::net::SocketAddrV6) -> Self {
        Self::V6(*socket.ip(), socket.scope_id())
    }
}
impl From<Ipv6Addr> for SIpAddr {
    fn from(ip: Ipv6Addr) -> Self {
        Self::V6(ip, 0)
    }
}
impl From<Ipv4Addr> for SIpAddr {
    fn from(ip: Ipv4Addr) -> Self {
        Self::V4(ip)
    }
}

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
            self.addr[0], self.addr[1], self.addr[2], self.addr[3], self.addr[4], self.addr[5]
        )
    }
}

impl FromStr for MacAddr {
    type Err = String;
    fn from_str(s: &str) -> Result<MacAddr, Self::Err> {
        match MacAddr::parse(s) {
            Some(mac) => Ok(mac),
            None => Err(format!("Invalid MAC address: '{s}'")),
        }
    }
}

impl MacAddr {
    /// Creates a new MAC address instance from address bytes.
    pub fn new(addr: [u8; 6]) -> Self {
        Self { addr }
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
    pub inet: InetConfig<std::net::SocketAddrV4>,
    /// IPv6 network address and netmask of this interface.
    pub inet6: InetConfig<std::net::SocketAddrV6>,
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

    /// Get the available ip address.
    pub(crate) fn ip(&self, prefer_ipv4: bool) -> Option<SIpAddr> {
        let (first, second) = if prefer_ipv4 {
            (
                self.inet.addr.map(Into::into),
                self.inet6.addr.map(Into::into),
            )
        } else {
            (
                self.inet6.addr.map(Into::into),
                self.inet.addr.map(Into::into),
            )
        };

        first.or(second)
    }

    /// Check if the given ip matches ours and if so dispose of the other kind of ip.
    pub(crate) fn matched_ip_kind(mut self, ip: std::net::IpAddr) -> Option<Self> {
        if match ip {
            std::net::IpAddr::V4(ip) => {
                self.inet6.addr = None;
                self.inet6.netmask = None;
                Some(&ip) == self.inet.addr.as_ref().map(|a| a.ip())
            }
            std::net::IpAddr::V6(ip) => {
                self.inet.addr = None;
                self.inet.netmask = None;
                Some(&ip) == self.inet6.addr.as_ref().map(|a| a.ip())
            }
        } {
            Some(self)
        } else {
            None
        }
    }

    /// Sort interfaces depending on ip v4/v6 preference and ipv6 types
    pub(crate) fn sort(&self, other: &Self, prefer_ipv4: bool) -> std::cmp::Ordering {
        if prefer_ipv4 {
            return self.inet.addr.is_some().cmp(&other.inet.addr.is_some());
        }
        if self.inet6.addr.is_some() != other.inet6.addr.is_some() {
            // prefer ipv6
            return self.inet6.addr.is_some().cmp(&other.inet6.addr.is_some());
        }
        fn is_unique_local(ip: &Ipv6Addr) -> bool {
            (ip.segments()[0] & 0xfe00) == 0xfc00
        }
        fn is_unicast_link_local(ip: &Ipv6Addr) -> bool {
            (ip.segments()[0] & 0xffc0) == 0xfe80
        }
        match (self.inet6.addr, other.inet6.addr) {
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(a), Some(b)) => {
                let a = a.ip();
                let b = b.ip();
                // prefer unique local addresses
                if is_unique_local(a) != is_unique_local(b) {
                    return is_unique_local(a).cmp(&is_unique_local(b));
                }
                // prefer link local
                if is_unicast_link_local(a) != is_unicast_link_local(b) {
                    return is_unicast_link_local(a).cmp(&is_unicast_link_local(b));
                }
                // prefer not unspecified
                if a.is_unspecified() != b.is_unspecified() {
                    return b.is_unspecified().cmp(&a.is_unspecified());
                }
                // prefer not multicast
                if a.is_multicast() != b.is_multicast() {
                    return b.is_multicast().cmp(&a.is_multicast());
                }
                std::cmp::Ordering::Equal
            }
            (None, None) => std::cmp::Ordering::Equal,
        }
    }

    /// Check if the given interface matches ours and if so dispose of the other kind of ip.
    pub fn matched_subnet_kind(
        mut self,
        net_addr: std::net::IpAddr,
        net_mask: u128,
    ) -> Option<Self> {
        if let Some((addr, mask)) = match (self.inet.addr, self.inet.netmask) {
            (Some(addr), Some(mask)) if net_addr.is_ipv4() => Some((addr, mask)),
            _ => None,
        } {
            let mask = mask.ip().to_bits();
            if mask as u128 != net_mask {
                return None;
            }

            let addr = addr.ip().to_bits();
            let subnet = addr & mask;

            if Ipv4Addr::from(subnet) == net_addr {
                self.inet6.addr = None;
                self.inet6.netmask = None;
                return Some(self);
            }
        }

        if let Some((addr, mask)) = match (self.inet6.addr, self.inet6.netmask) {
            (Some(addr), Some(mask)) if net_addr.is_ipv6() => Some((addr, mask)),
            _ => None,
        } {
            let mask = mask.ip().to_bits();
            if mask != net_mask {
                return None;
            }

            let addr = addr.ip().to_bits();
            let subnet = addr & mask;

            if Ipv6Addr::from(subnet) == net_addr {
                self.inet.addr = None;
                self.inet.netmask = None;
                return Some(self);
            }
        }

        None
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
    let mut nics = vec![];

    for addr in getifaddrs().unwrap() {
        let mut nic = Interface::new(&addr.interface_name);
        if let Some(sock) = addr.address {
            if let Some(sock) = sock.as_sockaddr_in() {
                nic.inet.addr = Some((*sock).into());
            }
            if let Some(sock) = sock.as_sockaddr_in6() {
                // Not supported for now!
                if sock.scope_id() > 0 {
                    continue;
                }
                nic.inet6.addr = Some((*sock).into());
            }
            if let Some(link) = sock.as_link_addr() {
                nic.mac = link.addr().map(MacAddr::new);
            }
        }

        if let Some(sock) = addr.netmask {
            if let Some(sock) = sock.as_sockaddr_in() {
                nic.inet.netmask = Some((*sock).into());
            }
            if let Some(sock) = sock.as_sockaddr_in6() {
                nic.inet6.netmask = Some((*sock).into());
            }
        }
        // Skip interfaces without ip's.
        if nic.inet.addr.is_some() || nic.inet6.addr.is_some() {
            nics.push(nic);
        }
    }

    nics
}

/// Utility to parse an IPv4 address string into a nix's Ipv4Addr.
pub fn parse_ip(addr: &str) -> Result<std::net::IpAddr, String> {
    addr.parse::<std::net::IpAddr>().map_err(|e| e.to_string())
}

/// Utility to parse an IPvX subnet string into a nix's IpvXAddr.
pub fn parse_ip_subnet(addr_str: &str) -> Result<(std::net::IpAddr, u128), String> {
    let (addr, bits) = match addr_str.split_once('/') {
        Some(p) => p,
        None => return Err(format!("Invalid subnet: '{addr_str}'")),
    };

    let bits = bits
        .parse::<u32>()
        .map_err(|e| format!("Invalid subnet '{addr_str}': {e}"))?;

    match parse_ip(addr)? {
        std::net::IpAddr::V4(v4) => {
            if bits > 32 {
                return Err(format!("Invalid subnet '{addr_str}': suffix too large"));
            }
            let addr = v4.to_bits();
            let mask = !0 << (32 - bits);
            let subnet = addr & mask;

            Ok((std::net::IpAddr::V4(Ipv4Addr::from(subnet)), mask as u128))
        }
        std::net::IpAddr::V6(v6) => {
            if bits > 128 {
                return Err(format!("Invalid subnet '{addr_str}': suffix too large"));
            }

            let addr = v6.to_bits();
            let mask = !0 << (128 - bits);
            let subnet = addr & mask;

            Ok((std::net::IpAddr::V6(Ipv6Addr::from(subnet)), mask))
        }
    }
}
