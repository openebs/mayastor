use crate::{BdevClient, MayaClient};
use byte_unit::Byte;
use clap::ArgMatches;
use std::cmp::max;

pub struct Context {
    pub(crate) client: MayaClient,
    pub(crate) bdev: BdevClient,
    verbosity: u64,
    units: char,
}

impl Context {
    pub(crate) async fn new(matches: &ArgMatches<'_>) -> Self {
        let verbosity = if matches.is_present("quiet") {
            0
        } else {
            matches.occurrences_of("verbose") + 1
        };
        let units = matches
            .value_of("units")
            .and_then(|u| u.chars().next())
            .unwrap_or('b');
        let endpoint = {
            let addr = matches.value_of("address").unwrap_or("127.0.0.1");
            let port = value_t!(matches.value_of("port"), u16).unwrap_or(10124);
            format!("{}:{}", addr, port)
        };
        let uri = format!("http://{}", endpoint);
        if verbosity > 1 {
            println!("Connecting to {}", uri);
        }

        let client = MayaClient::connect(uri.clone()).await.unwrap();
        Context {
            client,
            bdev: BdevClient::connect(uri).await.unwrap(),
            verbosity,
            units,
        }
    }
    pub(crate) fn v1(&self, s: &str) {
        if self.verbosity > 0 {
            println!("{}", s)
        }
    }

    pub(crate) fn v2(&self, s: &str) {
        if self.verbosity > 1 {
            println!("{}", s)
        }
    }

    pub(crate) fn units(&self, n: Byte) -> String {
        match self.units {
            'i' => n.get_appropriate_unit(true).to_string(),
            'd' => n.get_appropriate_unit(false).to_string(),
            _ => n.get_bytes().to_string(),
        }
    }

    pub(crate) fn print_list(
        &self,
        headers: Vec<&str>,
        mut data: Vec<Vec<String>>,
    ) {
        assert_ne!(data.len(), 0);
        let ncols = data.first().unwrap().len();
        assert_eq!(headers.len(), ncols);

        let columns = if self.verbosity > 0 {
            data.insert(
                0,
                headers
                    .iter()
                    .map(|h| {
                        (if h.starts_with('>') { &h[1 ..] } else { h })
                            .to_string()
                    })
                    .collect(),
            );

            data.iter().fold(
                headers
                    .iter()
                    .map(|h| (h.starts_with('>'), 0usize))
                    .collect(),
                |thus_far: Vec<(bool, usize)>, elem| {
                    thus_far
                        .iter()
                        .zip(elem)
                        .map(|((a, l), s)| (*a, max(*l, s.len())))
                        .collect()
                },
            )
        } else {
            vec![(false, 0usize); ncols]
        };

        for row in data {
            let vals = row.iter().enumerate().map(|(idx, s)| {
                if columns[idx].0 {
                    format!("{:>1$}", s, columns[idx].1)
                } else {
                    format!("{:<1$}", s, columns[idx].1)
                }
            });

            println!("{}", vals.collect::<Vec<String>>().join(" "));
        }
    }
}
