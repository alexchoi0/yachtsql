use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::super::IrEvaluator;

impl<'a> IrEvaluator<'a> {
    pub(crate) fn fn_md5(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let digest = md5::compute(s.as_bytes());
                Ok(Value::Bytes(digest.to_vec()))
            }
            Some(Value::Bytes(b)) => {
                let digest = md5::compute(b);
                Ok(Value::Bytes(digest.to_vec()))
            }
            _ => Err(Error::InvalidQuery("MD5 requires string argument".into())),
        }
    }

    pub(crate) fn fn_sha256(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(s.as_bytes());
                let result = hasher.finalize();
                Ok(Value::Bytes(result.to_vec()))
            }
            Some(Value::Bytes(b)) => {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(b);
                let result = hasher.finalize();
                Ok(Value::Bytes(result.to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "SHA256 requires string or bytes argument".into(),
            )),
        }
    }

    pub(crate) fn fn_sha512(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                use sha2::{Digest, Sha512};
                let mut hasher = Sha512::new();
                hasher.update(s.as_bytes());
                let result = hasher.finalize();
                Ok(Value::Bytes(result.to_vec()))
            }
            Some(Value::Bytes(b)) => {
                use sha2::{Digest, Sha512};
                let mut hasher = Sha512::new();
                hasher.update(b);
                let result = hasher.finalize();
                Ok(Value::Bytes(result.to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "SHA512 requires string or bytes argument".into(),
            )),
        }
    }

    pub(crate) fn fn_keys_new_keyset(&self, args: &[Value]) -> Result<Value> {
        use aes_gcm::aead::OsRng;
        use aes_gcm::aead::rand_core::RngCore;

        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "KEYS.NEW_KEYSET requires key type argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(key_type) => {
                let key_size = match key_type.to_uppercase().as_str() {
                    "AEAD_AES_GCM_256" => 32,
                    "AEAD_AES_GCM_128" => 16,
                    "DETERMINISTIC_AEAD_AES_SIV_CMAC_256" => 64,
                    _ => 32,
                };

                let mut key = vec![0u8; key_size];
                OsRng.fill_bytes(&mut key);

                let keyset = serde_json::json!({
                    "primaryKeyId": OsRng.next_u32(),
                    "keyType": key_type,
                    "key": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &key)
                });

                let keyset_bytes = serde_json::to_vec(&keyset).map_err(|e| {
                    Error::InvalidQuery(format!("Failed to serialize keyset: {}", e))
                })?;

                Ok(Value::Bytes(keyset_bytes))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.NEW_KEYSET expects a string key type argument".into(),
            )),
        }
    }

    pub(crate) fn fn_keys_add_key_from_raw_bytes(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "KEYS.ADD_KEY_FROM_RAW_BYTES requires keyset, key_type, and raw_bytes arguments"
                    .into(),
            ));
        }

        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
            (Value::Bytes(keyset), Value::String(_key_type), Value::Bytes(raw_bytes)) => {
                let mut keyset_json: serde_json::Value = serde_json::from_slice(keyset)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid keyset: {}", e)))?;

                if let Some(obj) = keyset_json.as_object_mut() {
                    obj.insert(
                        "additionalKey".to_string(),
                        serde_json::json!(base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            raw_bytes
                        )),
                    );
                }

                let new_keyset = serde_json::to_vec(&keyset_json).map_err(|e| {
                    Error::InvalidQuery(format!("Failed to serialize keyset: {}", e))
                })?;

                Ok(Value::Bytes(new_keyset))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.ADD_KEY_FROM_RAW_BYTES expects bytes arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_keys_keyset_chain(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "KEYS.KEYSET_CHAIN requires kms_resource and keyset arguments".into(),
            ));
        }

        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(_kms_resource), Value::Bytes(keyset)) => {
                Ok(Value::Bytes(keyset.clone()))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.KEYSET_CHAIN expects string and bytes arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_keys_keyset_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "KEYS.KEYSET_FROM_JSON requires json_string argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(json_str) => {
                let keyset_bytes = json_str.as_bytes().to_vec();
                Ok(Value::Bytes(keyset_bytes))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.KEYSET_FROM_JSON expects a string argument".into(),
            )),
        }
    }

    pub(crate) fn fn_keys_keyset_to_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "KEYS.KEYSET_TO_JSON requires keyset argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(keyset) => {
                let json_str = String::from_utf8_lossy(keyset).to_string();
                Ok(Value::String(json_str))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.KEYSET_TO_JSON expects a bytes argument".into(),
            )),
        }
    }

    pub(crate) fn fn_keys_keyset_length(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "KEYS.KEYSET_LENGTH requires keyset argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(keyset) => {
                let keyset_json: serde_json::Value =
                    serde_json::from_slice(keyset).unwrap_or(serde_json::json!({"keys": []}));

                let count = keyset_json
                    .get("keys")
                    .and_then(|k| k.as_array())
                    .map(|arr| arr.len())
                    .unwrap_or(1);

                Ok(Value::Int64(count as i64))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.KEYSET_LENGTH expects a bytes argument".into(),
            )),
        }
    }

    pub(crate) fn fn_keys_rotate_keyset(&self, args: &[Value]) -> Result<Value> {
        use aes_gcm::aead::OsRng;
        use aes_gcm::aead::rand_core::RngCore;

        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "KEYS.ROTATE_KEYSET requires keyset and key_type arguments".into(),
            ));
        }

        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Bytes(keyset), Value::String(key_type)) => {
                let mut keyset_json: serde_json::Value = serde_json::from_slice(keyset)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid keyset: {}", e)))?;

                let key_size = match key_type.to_uppercase().as_str() {
                    "AEAD_AES_GCM_256" => 32,
                    "AEAD_AES_GCM_128" => 16,
                    "DETERMINISTIC_AEAD_AES_SIV_CMAC_256" => 64,
                    _ => 32,
                };

                let mut new_key = vec![0u8; key_size];
                OsRng.fill_bytes(&mut new_key);

                if let Some(obj) = keyset_json.as_object_mut() {
                    obj.insert(
                        "primaryKeyId".to_string(),
                        serde_json::json!(OsRng.next_u32()),
                    );
                    obj.insert(
                        "key".to_string(),
                        serde_json::json!(base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            &new_key
                        )),
                    );
                }

                let new_keyset = serde_json::to_vec(&keyset_json).map_err(|e| {
                    Error::InvalidQuery(format!("Failed to serialize keyset: {}", e))
                })?;

                Ok(Value::Bytes(new_keyset))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.ROTATE_KEYSET expects bytes and string arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_aead_encrypt(&self, args: &[Value]) -> Result<Value> {
        use aes_gcm::aead::rand_core::RngCore;
        use aes_gcm::aead::{Aead, KeyInit, OsRng};
        use aes_gcm::{Aes256Gcm, Nonce};

        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "AEAD.ENCRYPT requires keyset, plaintext, and additional_data arguments".into(),
            ));
        }

        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
            (Value::Bytes(keyset), Value::Bytes(plaintext), aad) => {
                let keyset_json: serde_json::Value = serde_json::from_slice(keyset)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid keyset: {}", e)))?;

                let key_b64 = keyset_json
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| Error::InvalidQuery("Keyset missing key".into()))?;

                let key_bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64)
                        .map_err(|e| Error::InvalidQuery(format!("Invalid key encoding: {}", e)))?;

                let key: [u8; 32] = key_bytes.try_into().map_err(|_| {
                    Error::InvalidQuery("Key must be 32 bytes for AES-256-GCM".into())
                })?;

                let cipher = Aes256Gcm::new_from_slice(&key)
                    .map_err(|e| Error::InvalidQuery(format!("Failed to create cipher: {}", e)))?;

                let mut nonce_bytes = [0u8; 12];
                OsRng.fill_bytes(&mut nonce_bytes);
                let nonce = Nonce::from_slice(&nonce_bytes);

                let aad_bytes = match aad {
                    Value::Bytes(b) => b.clone(),
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Null => vec![],
                    _ => vec![],
                };

                let ciphertext = cipher
                    .encrypt(
                        nonce,
                        aes_gcm::aead::Payload {
                            msg: plaintext,
                            aad: &aad_bytes,
                        },
                    )
                    .map_err(|e| Error::InvalidQuery(format!("Encryption failed: {}", e)))?;

                let mut result = nonce_bytes.to_vec();
                result.extend(ciphertext);

                Ok(Value::Bytes(result))
            }
            _ => Err(Error::InvalidQuery(
                "AEAD.ENCRYPT expects bytes arguments".into(),
            )),
        }
    }

    pub(crate) fn fn_aead_decrypt_string(&self, args: &[Value]) -> Result<Value> {
        let decrypted = self.fn_aead_decrypt_bytes(args)?;
        match decrypted {
            Value::Null => Ok(Value::Null),
            Value::Bytes(b) => {
                let s = String::from_utf8(b).map_err(|e| {
                    Error::InvalidQuery(format!("Decrypted data is not valid UTF-8: {}", e))
                })?;
                Ok(Value::String(s))
            }
            _ => Ok(decrypted),
        }
    }

    pub(crate) fn fn_aead_decrypt_bytes(&self, args: &[Value]) -> Result<Value> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};

        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "AEAD.DECRYPT requires keyset, ciphertext, and additional_data arguments".into(),
            ));
        }

        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
            (Value::Bytes(keyset), Value::Bytes(ciphertext), aad) => {
                if ciphertext.len() < 12 {
                    return Err(Error::InvalidQuery("Ciphertext too short".into()));
                }

                let keyset_json: serde_json::Value = serde_json::from_slice(keyset)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid keyset: {}", e)))?;

                let key_b64 = keyset_json
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| Error::InvalidQuery("Keyset missing key".into()))?;

                let key_bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64)
                        .map_err(|e| Error::InvalidQuery(format!("Invalid key encoding: {}", e)))?;

                let key: [u8; 32] = key_bytes.try_into().map_err(|_| {
                    Error::InvalidQuery("Key must be 32 bytes for AES-256-GCM".into())
                })?;

                let cipher = Aes256Gcm::new_from_slice(&key)
                    .map_err(|e| Error::InvalidQuery(format!("Failed to create cipher: {}", e)))?;

                let nonce = Nonce::from_slice(&ciphertext[..12]);
                let encrypted_data = &ciphertext[12..];

                let aad_bytes = match aad {
                    Value::Bytes(b) => b.clone(),
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Null => vec![],
                    _ => vec![],
                };

                let plaintext = cipher
                    .decrypt(
                        nonce,
                        aes_gcm::aead::Payload {
                            msg: encrypted_data,
                            aad: &aad_bytes,
                        },
                    )
                    .map_err(|e| Error::InvalidQuery(format!("Decryption failed: {}", e)))?;

                Ok(Value::Bytes(plaintext))
            }
            _ => Err(Error::InvalidQuery(
                "AEAD.DECRYPT expects bytes arguments".into(),
            )),
        }
    }
}
