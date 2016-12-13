extern crate xattr;
extern crate byteorder;
extern crate uuid;

use uuid::Uuid;
use std::io::{Cursor, Error};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug)]
pub struct Xtime(u32, u32);

const BRICK_GFID_XATTR: &'static str = "trusted.gfid";
const VOLUME_ID_XATTR: &'static str = "trusted.glusterfs.volume-id";
const XTIME_STIME_XATTR_PREFIX: &'static str = "trusted.glusterfs";

fn get_xtime_stime (path: &str, xattr_name: &str) -> Result<Xtime, Error> {
    let v = try!(xattr::get(path, xattr_name));

    let mut rdr = Cursor::new(v);
    Ok(Xtime(rdr.read_u32::<BigEndian>().unwrap_or(0),
             rdr.read_u32::<BigEndian>().unwrap_or(0)))
}


fn set_xtime_stime (path: &str, xattr_name: &str, sec: u32, msec: u32) -> Result<(), Error> {
    let mut wtr = vec![];
    try!(wtr.write_u32::<BigEndian>(sec));
    try!(wtr.write_u32::<BigEndian>(msec));
    xattr::set(path, xattr_name, &wtr)
}


fn get_uuid (path: &str, xattr_name: &str) -> Result<String, Error> {
    let v = try!(xattr::get(path, xattr_name));
    let uuid = Uuid::from_bytes(&v);
    Ok(uuid.unwrap().hyphenated().to_string())
}


fn set_uuid (path: &str, xattr_name: &str, gfid: &str) -> Result<(), Error> {
    let uuid = Uuid::parse_str(gfid).unwrap();
    xattr::set(path, xattr_name, uuid.as_bytes())
}

/// Get GFID(`trusted.gfid`)
///
/// Examples:
///
/// ```
/// extern crate glusterxattr;
///
/// use glusterxattr::get_gfid;
///
/// fn main() {
///     let res = get_gfid("/bricks/b1/f1");
///     match res {
///         Ok(v) => println!("GFID: {}", v),
///         Err(e) => println!("Failed to get GFID: {}", e)
///     }
/// }
/// ```
pub fn get_gfid (path: &str) -> Result<String, Error> {
    get_uuid(path, BRICK_GFID_XATTR)
}

/// Set GFID(`trusted.gfid`)
///
/// Examples:
///
/// ```
/// extern crate glusterxattr;
///
/// use glusterxattr::set_gfid;
///
/// fn main() {
///     let res = set_gfid("/bricks/b1/f1", "0a118af0-3c20-4bdd-aded-694a17af6b5a");
///     match res {
///         Ok(_) => println!("OK"),
///         Err(e) => println!("Failed to set GFID: {}", e)
///     }
/// }
/// ```
pub fn set_gfid (path: &str, gfid: &str) -> Result<(), Error> {
    set_uuid(path, BRICK_GFID_XATTR, gfid)
}

/// Get Volume ID(`trusted.glusterfs.volume-id`)
///
/// Examples:
///
/// ```
/// extern crate glusterxattr;
///
/// use glusterxattr::get_volume_id;
///
/// fn main() {
///     let res = get_volume_id("/bricks/b1");
///     match res {
///         Ok(v) => println!("Volume ID: {}", v),
///         Err(e) => println!("Failed to get Volume ID: {}", e)
///     }
/// }
/// ```
pub fn get_volume_id (path: &str) -> Result<String, Error> {
    get_uuid(path, VOLUME_ID_XATTR)
}

/// Set Volume ID(`trusted.glusterfs.volume-id`)
///
/// Examples:
///
/// ```
/// extern crate glusterxattr;
///
/// use glusterxattr::set_volume_id;
///
/// fn main() {
///     let res = set_volume_id("/bricks/b1", "0a118af0-3c20-4bdd-aded-694a17af6b5a");
///     match res {
///         Ok(_) => println!("OK"),
///         Err(e) => println!("Failed to set volume ID: {}", e)
///     }
/// }
/// ```
pub fn set_volume_id (path: &str, volume_id: &str) -> Result<(), Error> {
    set_uuid(path, VOLUME_ID_XATTR, volume_id)
}

/// Get Xtime(`trusted.glusterfs.<mastervol_uuid>.xtime`)
///
/// Examples:
///
/// ```
/// extern crate glusterxattr;
///
/// use glusterxattr::get_xtime;
///
/// fn main() {
///     let res = get_xtime("/bricks/b1", "0a118af0-3c20-4bdd-aded-694a17af6b5a");
///     match res {
///         Ok(v) => println!("Xtime: {:?}", v),
///         Err(e) => println!("Failed to get Xtime: {}", e)
///     }
/// }
/// ```
pub fn get_xtime (path: &str, volume_id: &str) -> Result<Xtime, Error> {
    let xattr_name = format!("{}.{}.xtime", XTIME_STIME_XATTR_PREFIX, volume_id);
    let xattr_name = xattr_name.as_str();
    get_xtime_stime (path, xattr_name)
}

/// Set Xtime(`trusted.glusterfs.<mastervol_uuid>.xtime`)
///
/// Examples:
///
/// ```
/// extern crate glusterxattr;
///
/// use glusterxattr::set_xtime;
///
/// fn main() {
///     let res = set_xtime("/bricks/b1", "0a118af0-3c20-4bdd-aded-694a17af6b5a",
///                         1481540557, 016683);
///     match res {
///         Ok(_) => println!("OK"),
///         Err(e) => println!("Failed to set xtime: {}", e)
///     }
/// }
/// ```
pub fn set_xtime (path: &str, volume_id: &str, sec: u32, msec: u32) -> Result<(), Error> {
    let xattr_name = format!("{}.{}.xtime", XTIME_STIME_XATTR_PREFIX, volume_id);
    let xattr_name = xattr_name.as_str();
    set_xtime_stime (path, xattr_name, sec, msec)
}

/// Get Stime(`trusted.glusterfs.<mastervol_uuid>.<slavevol_uuid>.stime`)
///
/// Examples:
///
/// ```
/// extern crate glusterxattr;
///
/// use glusterxattr::get_stime;
///
/// fn main() {
///     let res = get_stime("/bricks/b1", "0a118af0-3c20-4bdd-aded-694a17af6b5a",
///                         "af95963b-bbe6-49cb-bf6d-db7260ea6f72");
///     match res {
///         Ok(v) => println!("Stime: {:?}", v),
///         Err(e) => println!("Failed to get Stime: {}", e)
///     }
/// }
/// ```
pub fn get_stime (path: &str, master_volume_id: &str, slave_volume_id: &str) -> Result<Xtime, Error> {
    let xattr_name = format!("{}.{}.{}.stime", XTIME_STIME_XATTR_PREFIX, master_volume_id, slave_volume_id);
    let xattr_name = xattr_name.as_str();
    get_xtime(path, xattr_name)
}

/// Set Stime(`trusted.glusterfs.<mastervol_uuid>.<slavevol_uuid>.stime`)
///
/// Examples:
///
/// ```
/// extern crate glusterxattr;
///
/// use glusterxattr::set_stime;
///
/// fn main() {
///     let res = set_stime("/bricks/b1", "0a118af0-3c20-4bdd-aded-694a17af6b5a",
///                         "af95963b-bbe6-49cb-bf6d-db7260ea6f72",
///                         1481540557, 016683);
///     match res {
///         Ok(_) => println!("OK"),
///         Err(e) => println!("Failed to set stime: {}", e)
///     }
/// }
/// ```
pub fn set_stime(path: &str, master_volume_id: &str, slave_volume_id: &str, sec: u32, msec: u32) -> Result<(), Error> {
    let xattr_name = format!("{}.{}.{}.stime", XTIME_STIME_XATTR_PREFIX, master_volume_id, slave_volume_id);
    let xattr_name = xattr_name.as_str();
    set_xtime(path, xattr_name, sec, msec)
}


#[test]
fn test_set_and_get_xtime_stime() {
    let xattr_name = "user.glusterfs.f9b3a729-872f-4535-ae41-45ee7c62f223.xtime";
    assert_eq!((), set_xtime_stime("./testfile", xattr_name, 100, 2).unwrap());
    let val = get_xtime_stime("./testfile", xattr_name).unwrap();
    assert_eq!((100, 2), (val.0, val.1));
}

#[test]
fn test_set_and_get_uuid(){
    assert_eq!((), set_uuid("./testfile", "user.gfid", "bb74c663-2552-41aa-a0ae-d4d94d9dd187").unwrap());
    let val = get_uuid("./testfile", "user.gfid").unwrap();
    assert_eq!("bb74c663-2552-41aa-a0ae-d4d94d9dd187", val);
}
