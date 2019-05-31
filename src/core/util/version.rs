// Copyright 2019 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::str::FromStr;
use std::string::ToString;

use error::{ErrorKind::IllegalArgument, Result};
/// Use by certain classes to match version compatibility
/// across releases of Lucene.
///
/// <p><b>WARNING</b>: When changing the version parameter
/// that you supply to components in Lucene, do not simply
/// change the version at search-time, but instead also adjust
/// your indexing code to match, and re-index.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Hash)]
pub struct Version {
    /// Major version, the difference between stable and trunk */
    pub major: i32,
    /// Minor version, incremented within the stable branch */
    pub minor: i32,
    /// Bugfix number, incremented on release branches */
    pub bugfix: i32,
    /// Prerelease version, currently 0 (alpha), 1 (beta), or 2 (final) */
    pub prerelease: i32,
}

pub const RUCENE_VERSION_6_4_18: Version = Version {
    major: 6,
    minor: 4,
    bugfix: 18,
    prerelease: 0,
};

pub const VERSION_LATEST: Version = RUCENE_VERSION_6_4_18;

impl Version {
    /// Parse a version number of the form {@code "major.minor.bugfix.prerelease"}.
    ///
    /// Part {@code ".bugfix"} and part {@code ".prerelease"} are optional.
    /// Note that this is forwards compatible: the parsed version does not have to exist as
    /// a constant.
    ///
    /// @lucene.internal
    pub fn with_string(version: &str) -> Result<Version> {
        let splited: Vec<&str> = version.split('.').collect();
        if splited.len() < 2 {
            bail!(IllegalArgument(format!(
                "Version is not in form major.minor.bugfix(.prerelease) (got: {})",
                version
            )));
        }
        let major = i32::from_str(splited[0])?;
        let minor = i32::from_str(splited[1])?;
        let bugfix = if splited.len() > 2 {
            i32::from_str(splited[2])?
        } else {
            0
        };
        let prerelease = if splited.len() > 3 {
            i32::from_str(splited[3])?
        } else {
            0
        };

        Self::with_bits(major, minor, bugfix, prerelease)
    }

    pub fn with_string_leniently(version: &str) -> Result<Version> {
        match version {
            "LATEST" => Ok(VERSION_LATEST),
            "LUCENE_CURRENT" => Ok(VERSION_LATEST),
            _ => {
                if !version.starts_with("LUCENE_") {
                    bail!(IllegalArgument(format!(
                        "Version is not starts with LUCENE_ (got: {})",
                        version
                    )));
                }

                let v = version.replace("LUCENE_", "").replace("_", ".");
                let splited: Vec<&str> = v.split('.').collect();

                let mut major;
                let mut minor = 0;

                if splited.len() == 1 {
                    let num = i32::from_str(splited[0])?;
                    major = num;
                    if num > 10 && num < 100 {
                        major = num / 10;
                        minor = num % 10;
                    }
                } else {
                    major = i32::from_str(splited[0])?;
                    minor = i32::from_str(splited[1])?;
                }

                let bugfix = if splited.len() > 2 {
                    i32::from_str(splited[2])?
                } else {
                    0
                };
                let prerelease = if splited.len() > 3 {
                    i32::from_str(splited[3])?
                } else {
                    0
                };

                Self::with_bits(major, minor, bugfix, prerelease)
            }
        }
    }

    pub fn new(major: i32, minor: i32, bugfix: i32) -> Result<Version> {
        Self::with_bits(major, minor, bugfix, 0)
    }

    pub fn with_bits(major: i32, minor: i32, bugfix: i32, prerelease: i32) -> Result<Version> {
        // NOTE: do not enforce major version so we remain future proof, except to
        // make sure it fits in the 8 bits we encode it into:
        if major > 255 || major < 0 {
            bail!(IllegalArgument(format!("Illegal major version: {}", major)));
        }
        if minor > 255 || minor < 0 {
            bail!(IllegalArgument(format!("Illegal minor version: {}", minor)));
        }
        if bugfix > 255 || bugfix < 0 {
            bail!(IllegalArgument(format!(
                "Illegal bugfix version: {}",
                bugfix
            )));
        }
        if prerelease > 2 || prerelease < 0 {
            bail!(IllegalArgument(format!(
                "Illegal pre-release version: {}",
                prerelease
            )));
        }
        if prerelease != 0 && (minor != 0 || bugfix != 0) {
            bail!(IllegalArgument(format!(
                "pre-release version only supported with major release (got pre-release: {}, \
                 minor: {}, bugfix: {}",
                prerelease, minor, bugfix
            )));
        }

        let version = Version {
            major,
            minor,
            bugfix,
            prerelease,
        };

        Ok(version)
    }

    // stores the version pieces, with most significant pieces in high bits
    // ie:  | 1 byte | 1 byte | 1 byte |   2 bits   |
    //         major   minor    bugfix   prerelease
    pub fn encode(&self) -> i32 {
        self.major << 18 | self.minor << 10 | self.bugfix << 2 | self.prerelease
    }

    /// Returns true if this version is the same or after the version from the argument.
    pub fn on_or_after(&self, other: &Version) -> bool {
        self.encode() >= other.encode()
    }
}

impl ToString for Version {
    fn to_string(&self) -> String {
        if self.prerelease == 0 {
            format!("{}.{}.{}", self.major, self.minor, self.bugfix)
        } else {
            format!(
                "{}.{}.{}.{}",
                self.major, self.minor, self.bugfix, self.prerelease
            )
        }
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.encode().cmp(&other.encode())
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Version) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[test]
fn test_with_string_leniently() {
    let version = "LUCENE_CURRENT";
    let v = Version::with_string_leniently(version).unwrap();
    assert_eq!(
        v,
        Version {
            major: 6,
            minor: 4,
            bugfix: 18,
            prerelease: 0,
        }
    );

    let version = "LUCENE_6_6_1_0";
    let v = Version::with_string_leniently(version).unwrap();
    assert_eq!(
        v,
        Version {
            major: 6,
            minor: 6,
            bugfix: 1,
            prerelease: 0,
        }
    );

    let version = "LUCENE_66";
    let v = Version::with_string_leniently(version).unwrap();
    assert_eq!(
        v,
        Version {
            major: 6,
            minor: 6,
            bugfix: 0,
            prerelease: 0,
        }
    );
}
