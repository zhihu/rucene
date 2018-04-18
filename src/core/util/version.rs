use std::cmp::Eq;
use std::str::FromStr;
use std::string::ToString;

use error::*;
/// Use by certain classes to match version compatibility
/// across releases of Lucene.
///
/// <p><b>WARNING</b>: When changing the version parameter
/// that you supply to components in Lucene, do not simply
/// change the version at search-time, but instead also adjust
/// your indexing code to match, and re-index.
///
#[derive(Clone, Debug, Serialize)]
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

const LATEST: Version = Version {
    major: 6,
    minor: 6,
    bugfix: 1,
    prerelease: 0,
};

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
            bail!(
                "Parse Error: Version is not in form major.minor.bugfix(.prerelease) (got: {})",
                version
            );
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
            "LATEST" => Ok(LATEST),
            "LUCENE_CURRENT" => Ok(LATEST),
            _ => {
                if !version.starts_with("LUCENE_") {
                    bail!(
                        "Parse Error: Version is not starts with LUCENE_ (got: {})",
                        version
                    );
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
            bail!("Illegal Argument: Illegal major version: {}", major);
        }
        if minor > 255 || minor < 0 {
            bail!("Illegal Argument: Illegal minor version: {}", minor);
        }
        if bugfix > 255 || bugfix < 0 {
            bail!("Illegal Argument: Illegal bugfix version: {}", bugfix);
        }
        if prerelease > 2 || prerelease < 0 {
            bail!(
                "Illegal Argument: Illegal prerelease version: {}",
                prerelease
            );
        }
        if prerelease != 0 && (minor != 0 || bugfix != 0) {
            bail!(
                "Illegal Argument: Prerelease version only supported with major release (got \
                 prerelease: {}, minor: {}, bugfix: {}",
                prerelease,
                minor,
                bugfix
            );
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
    ///
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

impl PartialEq for Version {
    fn eq(&self, other: &Version) -> bool {
        self.encode() == other.encode()
    }
}

impl Eq for Version {}

#[test]
fn test_with_string_leniently() {
    let version = "LUCENE_CURRENT";
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
