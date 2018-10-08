use multimap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_cbor;
use serde_json;
use std;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::hash::Hash;
use std::io::{Error, ErrorKind, Read, Write};
use std::path::Path;

pub fn to_json<T: Serialize>(obj: &T) -> String {
    serde_json::to_string_pretty(obj).unwrap()
}

pub fn write_json<T: Serialize>(path: &str, obj: &T) -> Result<(), Error> {
    std::fs::create_dir_all(std::path::Path::new(path).parent().unwrap())
        .expect("Creating parent dir failed");

    let mut file = File::create(path)?;
    file.write_all(to_json(obj).as_bytes())?;
    Ok(())
}

pub fn read_json<T: DeserializeOwned>(path: &str) -> Result<T, Error> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let obj: T = serde_json::from_str(&contents)?;
    Ok(obj)
}

pub fn write_binary<T: Serialize>(path: &str, obj: &T) -> Result<(), Error> {
    std::fs::create_dir_all(std::path::Path::new(path).parent().unwrap())
        .expect("Creating parent dir failed");

    let mut file = File::create(path)?;
    serde_cbor::to_writer(&mut file, obj).map_err(|err| Error::new(ErrorKind::Other, err))
}

pub fn read_binary<T: DeserializeOwned>(path: &str) -> Result<T, Error> {
    let file = File::open(path)?;
    let obj: T = serde_cbor::from_reader(file).map_err(|err| Error::new(ErrorKind::Other, err))?;
    Ok(obj)
}

// For BTreeMaps with struct keys. See https://github.com/serde-rs/json/issues/402.

pub fn serialize_btreemap<S: Serializer, K: Serialize, V: Serialize>(
    map: &BTreeMap<K, V>,
    s: S,
) -> Result<S::Ok, S::Error> {
    map.iter()
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect::<Vec<(_, _)>>()
        .serialize(s)
}

pub fn deserialize_btreemap<
    'de,
    D: Deserializer<'de>,
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
>(
    d: D,
) -> Result<BTreeMap<K, V>, D::Error> {
    let vec = <Vec<(K, V)>>::deserialize(d)?;
    let mut map = BTreeMap::new();
    for (k, v) in vec {
        map.insert(k, v);
    }
    Ok(map)
}

pub fn serialize_multimap<S: Serializer, K: Serialize + Eq + Hash, V: Serialize + Eq + Hash>(
    map: &multimap::MultiMap<K, V>,
    s: S,
) -> Result<S::Ok, S::Error> {
    // TODO maybe need to sort to have deterministic output
    map.iter_all()
        .map(|(key, values)| (key.clone(), values.clone()))
        .collect::<Vec<(_, _)>>()
        .serialize(s)
}

pub fn deserialize_multimap<
    'de,
    D: Deserializer<'de>,
    K: Deserialize<'de> + Eq + Hash + Clone,
    V: Deserialize<'de> + Eq + Hash,
>(
    d: D,
) -> Result<multimap::MultiMap<K, V>, D::Error> {
    let vec = <Vec<(K, Vec<V>)>>::deserialize(d)?;
    let mut map = multimap::MultiMap::new();
    for (key, values) in vec {
        for value in values {
            map.insert(key.clone(), value);
        }
    }
    Ok(map)
}

// Just list all things from a directory, return sorted by name, with file extension removed.
// Pretty hacky that we return a (String, String).
pub fn list_all_objects(dir: &str, map_name: &str) -> Vec<(String, String)> {
    let mut results: BTreeSet<(String, String)> = BTreeSet::new();
    match std::fs::read_dir(format!("../data/{}/{}/", dir, map_name)) {
        Ok(iter) => {
            for entry in iter {
                let name = Path::new(&entry.unwrap().file_name())
                    .file_stem()
                    .unwrap()
                    .to_os_string()
                    .into_string()
                    .unwrap();
                results.insert((name.clone(), name));
            }
        }
        Err(ref e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => panic!(e),
    };
    results.into_iter().collect()
}

// Load all serialized things from a directory, return sorted by name.
pub fn load_all_objects<T: DeserializeOwned>(dir: &str, map_name: &str) -> Vec<(String, T)> {
    let mut tree: BTreeMap<String, T> = BTreeMap::new();
    match std::fs::read_dir(format!("../data/{}/{}/", dir, map_name)) {
        Ok(iter) => {
            for entry in iter {
                let name = entry.unwrap().file_name().into_string().unwrap();
                let load: T = read_json(&format!("../data/{}/{}/{}", dir, map_name, name)).unwrap();
                tree.insert(name, load);
            }
        }
        Err(ref e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => panic!(e),
    };
    tree.into_iter().collect()
}

pub fn save_object<T: Serialize>(dir: &str, map_name: &str, obj_name: &str, obj: &T) {
    let path = format!("../data/{}/{}/{}.json", dir, map_name, obj_name);
    write_json(&path, obj).expect(&format!("Saving {} failed", path));
    println!("Saved {}", path);
}
