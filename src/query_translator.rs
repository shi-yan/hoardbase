use serde_json::Value;
use serde_json::json;

pub struct QueryTranslator {}

impl QueryTranslator {
    pub fn query_document(&self, query: &serde_json::Value , params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        let mut term_count = 0;

        let mut result = String::new();

        if let Some(query_doc) = query.as_object() {
            for (key, value) in query_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$or" => {
                            if value.is_array() {
                                if let Ok(res) = self.or(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" AND ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $or: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $or: {}", value));
                            }
                        }
                        "$and" => {
                            if value.is_array() {
                                if let Ok(res) = self.and(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" AND ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $and: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $and: {}", value));
                            }
                        }
                        "$not" => {
                            if value.is_object() {
                                if let Ok(res) = self.not(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" AND ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $not: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $not: {}", value));
                            }
                        }
                        "$nor" => {
                            if value.is_array() {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(" OR ");
                                    }

                                    in_values.push_str(self.nested("", val, params).unwrap().as_str());
                                }

                                result.push_str(&format!("NOT ({}) ", &in_values));
                                term_count += 1;
                            } else {
                                return Err(format!("Error in $nor: {}", value));
                            }
                        }
                        _ => {
                            return Err(format!("Unsupported operator: {}", key));
                        }
                    }
                } else {
                    if value.is_object() {
                        if key == "_id" {
                            return Err(format!("_id cannot be object"));
                        } else if let Ok(res) = self.nested(key, value, params) {
                            if term_count > 0 {
                                result.push_str(" AND ");
                            }

                            result.push_str(&res);
                            term_count += 1;
                        } else {
                            return Err(format!("Error in nested query: {}", value));
                        }
                    } else if value.is_array() {
                    } else if value.is_string() || value.is_number() || value.is_boolean() || value.is_f64() || value.is_i64() || value.is_u64() {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        match key.as_str() {
                            "_id" => {
                                result.push_str(&format!("{} = '{}'", key, value));
                            }
                            _ => {
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                            }
                        }
                        term_count += 1;
                    } else if value.is_null() {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        match key.as_str() {
                            "_id" => {
                                return Err(format!("_id cannot be null"));
                            }
                            _ => {
                                result.push_str(&format!("json_field('{}', raw) IS NULL", key));
                            }
                        }
                        term_count += 1;
                    } else {
                        return Err(format!("Unsupported type: {}", value));
                    }
                }
            }
        } else {
            return Err("Query is not an object".to_string());
        }

        Ok(result)
    }

    fn value(&self, value: &serde_json::Value,  params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        if value.is_string() {
            params.push(rusqlite::types::Value::from(String::from(value.as_str().unwrap())));
        } else if value.is_number() {
            params.push(rusqlite::types::Value::from(value.as_f64().unwrap()));
        } else if value.is_boolean() {
            params.push(rusqlite::types::Value::from(value.as_bool().unwrap()));
        } else if value.is_f64() {
            params.push(rusqlite::types::Value::from(value.as_f64().unwrap()));
        } else if value.is_i64() {
            params.push(rusqlite::types::Value::from(value.as_i64().unwrap()));
        } else if value.is_u64() {
            params.push(rusqlite::types::Value::from(value.as_i64().unwrap()));
        } else if value.is_null() {
            params.push(rusqlite::types::Value::Null);
        } else {
            return Err(format!("Unsupported type: {}", value));
        }
        Ok(format!("?{}", params.len()))
    }

    fn nested(&self, scope: &str, value: &serde_json::Value, params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        if let Some(value_doc) = value.as_object() {
            for (key, value) in value_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$lt" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $lt: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) < {}", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $lt: {}", value));
                            }
                        }
                        "$gt" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $gt: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) > {}", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $gt: {}", value));
                            }
                        }
                        "$gte" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $gte: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) >= {}", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $gte: {}", value));
                            }
                        }
                        "$eq" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $eq: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) = {}", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $eq: {}", value));
                            }
                        }
                        "$in" => {
                            if value.is_array() {
                                if term_count > 0 {
                                    return Err(format!("Error in $in: {}", value));
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(", ");
                                    }

                                    in_values.push_str(self.value(val, params).unwrap().as_str());
                                }
                                return Ok(format!("json_field('{}', raw) IN ({})", scope, in_values));
                            } else {
                                return Err(format!("Error in $in: {}", value));
                            }
                        }
                        "$lte" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $lte: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) <= {}", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $lte: {}", value));
                            }
                        }
                        "$ne" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $ne: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) != {}", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $ne: {}", value));
                            }
                        }
                        "$nin" => {
                            if value.is_array() {
                                if term_count > 0 {
                                    return Err(format!("Error in $nin: {}", value));
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(", ");
                                    }

                                    in_values.push_str(self.value(val, params).unwrap().as_str());
                                }
                                return Ok(format!("json_field('{}', raw) NOT IN ({})", scope, in_values));
                            } else {
                                return Err(format!("Error in $nin: {}", value));
                            }
                        }
                        "$exists" => {
                            if value.is_boolean() {
                                if term_count > 0 {
                                    return Err(format!("Error in $exists: {}", value));
                                }

                                return Ok(format!("json_field_exists('{}', raw) = {}", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $exists: {}", value));
                            }
                        }
                        "$type" => {
                            if value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $exists: {}", value));
                                }

                                return Ok(format!("json_field_type('{}', raw) = {}", scope, self.value(value, params ).unwrap()));
                            } else if value.is_array() {
                                if term_count > 0 {
                                    return Err(format!("Error in $exists: {}", value));
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if !val.is_string() {
                                        return Err(format!("Error in $exists: {}", value));
                                    } else {
                                        if in_values.len() > 0 {
                                            in_values.push_str(", ");
                                        }

                                        in_values.push_str(self.value(val, params).unwrap().as_str());
                                    }
                                }

                                return Ok(format!("json_field_type('{}', raw) IN ({})", scope, in_values));
                            }
                        }
                        "$size" => {
                            if value.is_number() {
                                if term_count > 0 {
                                    return Err(format!("Error in $size: {}", value));
                                }

                                return Ok(format!("json_field_size('{}', raw) = {}", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $size: {}", value));
                            }
                        }
                        "$all" => {
                            if value.is_array() {
                                if term_count > 0 {
                                    return Err(format!("Error in $all: {}", value));
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(" AND ");
                                    }

                                    in_values.push_str(&format!("json_field('{}', raw) = {}", scope, self.value(val, params).unwrap()));
                                }

                                return Ok(format!("({})", in_values.as_str()));
                            } else {
                                return Err(format!("Error in $all: {}", value));
                            }
                        }
                        "$elemMatch" => {
                            if value.is_array() {
                                if term_count > 0 {
                                    return Err(format!("Error in $elemMatch: {}", value));
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(", ");
                                    }

                                    in_values.push_str(&format!("{}", self.value(val, params).unwrap()));
                                }

                                return Ok(format!("json_field('{}', raw) IN ({})", scope, in_values.as_str()));
                            } else {
                                return Err(format!("Error in $elemMatch: {}", value));
                            }
                        }
                        "$bitsAllClear" => {
                            if value.is_u64() {
                                if term_count > 0 {
                                    return Err(format!("Error in $bitsAllClear: {}", value));
                                }

                                return Ok(format!("json_field_bits_all_clear('{}', raw, {})", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $bitsAllClear: {}", value));
                            }
                        }
                        "$bitsAllSet" => {
                            if value.is_u64() {
                                if term_count > 0 {
                                    return Err(format!("Error in $bitsAllSet: {}", value));
                                }

                                return Ok(format!("json_field_bits_all_set('{}', raw, {})", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $bitsAllSet: {}", value));
                            }
                        }
                        "$bitsAnyClear" => {
                            if value.is_u64() {
                                if term_count > 0 {
                                    return Err(format!("Error in $bitsAnyClear: {}", value));
                                }

                                return Ok(format!("json_field_bits_any_clear('{}', raw, {})", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $bitsAnyClear: {}", value));
                            }
                        }
                        "$bitsAnySet" => {
                            if value.is_u64() {
                                if term_count > 0 {
                                    return Err(format!("Error in $bitsAnySet: {}", value));
                                }

                                return Ok(format!("json_field_bits_any_set('{}', raw, {})", scope, self.value(value, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $bitsAnySet: {}", value));
                            }
                        }
                        "$mod" => {
                            if value.is_array() {
                                if value.as_array().unwrap().len() != 2 {
                                    return Err(format!("Error in $mod: {}", value));
                                }

                                let divisor = value.as_array().unwrap()[0].clone();
                                let remainder = value.as_array().unwrap()[1].clone();

                                if divisor == 0 {
                                    return Err(format!("Error in $mod: {}, Divisor can't be zero.", value));
                                }

                                return Ok(format!("json_field('{}', raw) % {} = {}", scope, self.value(&divisor, params ).unwrap(), self.value(&remainder, params ).unwrap()));
                            } else {
                                return Err(format!("Error in $mod: {}", value));
                            }
                        }
                        //todo $jsonSchema and $text not implemented
                        "$regex" => {
                            if value.is_string() {
                                let mut options = String::new();
                                if value_doc.keys().len() > 1 {
                                    if let Some(option_obj) = value_doc.get("$options") {
                                        if option_obj.is_string() {
                                            options = option_obj.as_str().unwrap().to_string();
                                        } else {
                                            return Err(format!("Error in $regex: {}", value));
                                        }
                                    }
                                }

                                let regex = value;

                                return Ok(format!("json_field_regex('{}', raw, {}, {})", scope, self.value(regex, params ).unwrap(), self.value( &json!(options), params ).unwrap()));
                            } else {
                                return Err(format!("Error in $regex: {}", value));
                            }
                        }
                        _ => {}
                    }
                } else {
                    if value.is_object() {
                        if let Ok(res) = self.nested(key, value, params) {
                            if term_count > 0 {
                                result.push_str(" AND ");
                            }

                            result.push_str(&res);
                            term_count += 1;
                        } else {
                            return Err(format!("Error in nested query: {}", value));
                        }
                    } else if value.is_array() {
                    } else if value.is_string() || value.is_number() || value.is_boolean() || value.is_f64() || value.is_i64() || value.is_u64() {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }

                        result.push_str(&format!("json_field('{}.{}', raw) = {}", scope, key, self.value(value, params).unwrap()));
                        term_count += 1;
                    } else if value.is_null() {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        result.push_str(&format!("json_field('{}.{}', raw) IS NULL", scope, key));
                        term_count += 1;
                    } else {
                        return Err(format!("Unsupported type: {}", value));
                    }
                }
            }
        } else {
            return Err("Query is not an object".to_string());
        }

        Ok(result)
    }

    fn or(&self, value: &serde_json::Value, params: &mut Vec<rusqlite::types::Value> ) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        if let Some(value_doc) = value.as_object() {
            for (key, value) in value_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$or" => {
                            if value.is_array() {
                                if let Ok(res) = self.or(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" OR ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $or: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $or: {}", value));
                            }
                        }
                        "$and" => {
                            if value.is_array() {
                                if let Ok(res) = self.and(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" OR ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $and: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $and: {}", value));
                            }
                        }
                        "$not" => {
                            if value.is_object() {
                                if let Ok(res) = self.not(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" OR ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $not: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $not: {}", value));
                            }
                        }
                        "$nor" => {
                            if value.is_array() {
                                if term_count > 0 {
                                    result.push_str(" OR ");
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(" OR ");
                                    }

                                    in_values.push_str(self.nested("", val, params).unwrap().as_str());
                                }

                                result.push_str(&format!("NOT ({}) ", &in_values));
                                term_count += 1;
                            } else {
                                return Err(format!("Error in $nor: {}", value));
                            }
                        }
                        _ => {
                            return Err(format!("Unsupported operator: {}", key));
                        }
                    }
                } else {
                    if value.is_object() {
                        if let Ok(res) = self.nested(key, value, params) {
                            if term_count > 0 {
                                result.push_str(" OR ");
                            }

                            result.push_str(&res);
                            term_count += 1;
                        } else {
                            return Err(format!("Error in nested query: {}", value));
                        }
                    } else if value.is_array() {
                    } else if value.is_string() || value.is_number() || value.is_boolean() || value.is_f64() || value.is_i64() || value.is_u64() {
                        if term_count > 0 {
                            result.push_str(" OR ");
                        }

                        result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                        term_count += 1;
                    } else if value.is_null() {
                        if term_count > 0 {
                            result.push_str(" OR ");
                        }
                        result.push_str(&format!("json_field('{}', raw) IS NULL", key));
                        term_count += 1;
                    } else {
                        return Err(format!("Unsupported type: {}", value));
                    }
                }
            }
        }
        Ok(result)
    }

    fn and(&self, value: &serde_json::Value, params: &mut Vec<rusqlite::types::Value> ) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        if let Some(value_doc) = value.as_object() {
            for (key, value) in value_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$or" => {
                            if value.is_array() {
                                if let Ok(res) = self.or(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" AND ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $or: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $or: {}", value));
                            }
                        }
                        "$and" => {
                            if value.is_array() {
                                if let Ok(res) = self.and(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" AND ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $and: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $and: {}", value));
                            }
                        }
                        "$not" => {
                            if value.is_object() {
                                if let Ok(res) = self.not(value, params) {
                                    if term_count > 0 {
                                        result.push_str(" AND ");
                                    }
                                    result.push_str(&format!("({})", &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $not: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $not: {}", value));
                            }
                        }
                        "$nor" => {
                            if value.is_array() {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(" OR ");
                                    }

                                    in_values.push_str(self.nested("", val, params).unwrap().as_str());
                                }

                                result.push_str(&format!("NOT ({}) ", &in_values));
                                term_count += 1;
                            } else {
                                return Err(format!("Error in $nor: {}", value));
                            }
                        }
                        _ => {
                            return Err(format!("Unsupported operator: {}", key));
                        }
                    }
                } else {
                    if value.is_object() {
                        if let Ok(res) = self.nested(key, value, params) {
                            if term_count > 0 {
                                result.push_str(" AND ");
                            }

                            result.push_str(&res);
                            term_count += 1;
                        } else {
                            return Err(format!("Error in nested query: {}", value));
                        }
                    } else if value.is_array() {
                    } else if value.is_string() || value.is_number() || value.is_boolean() || value.is_f64() || value.is_i64() || value.is_u64() {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }

                        result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                        term_count += 1;
                    } else if value.is_null() {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        result.push_str(&format!("json_field('{}', raw) IS NULL", key));
                        term_count += 1;
                    } else {
                        return Err(format!("Unsupported type: {}", value));
                    }
                }
            }
        }
        Ok(result)
    }

    fn not(&self, value: &serde_json::Value, params: &mut Vec<rusqlite::types::Value> ) -> Result<String, String> {
        let mut result = String::new();
        if let Some(value_doc) = value.as_object() {
            for (key, value) in value_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$or" => {
                            if value.is_array() {
                                if let Ok(res) = self.or(value, params) {
                                    result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                                } else {
                                    return Err(format!("Error in $or: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $or: {}", value));
                            }
                        }
                        "$and" => {
                            if value.is_array() {
                                if let Ok(res) = self.and(value, params) {
                                    result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                                } else {
                                    return Err(format!("Error in $and: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $and: {}", value));
                            }
                        }
                        "$not" => {
                            if value.is_object() {
                                if let Ok(res) = self.not(value, params) {
                                    result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                                } else {
                                    return Err(format!("Error in $not: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $not: {}", value));
                            }
                        }
                        "$nor" => {
                            if value.is_array() {
                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(" OR ");
                                    }

                                    in_values.push_str(self.nested("", val, params).unwrap().as_str());
                                }

                                result.push_str(&format!("NOT ({}) ", &in_values));
                            } else {
                                return Err(format!("Error in $nor: {}", value));
                            }
                        }
                        _ => {
                            return Err(format!("Unsupported operator: {}", key));
                        }
                    }
                } else {
                    if value.is_object() {
                        if let Ok(res) = self.nested(key, value, params) {
                            result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                        } else {
                            return Err(format!("Error in nested query: {}", value));
                        }
                    } else if value.is_array() {
                        return Err(format!("Unsupported type: {}", value));
                    } else if value.is_string() || value.is_number() || value.is_boolean() || value.is_f64() || value.is_i64() || value.is_u64() {
                        result.push_str(&format!("json_field('{}', raw) IS NOT {}", key, self.value(value, params).unwrap()));
                    } else if value.is_null() {
                        result.push_str(&format!("json_field('{}', raw) IS NOT NULL", key));
                    } else {
                        return Err(format!("Unsupported type: {}", value));
                    }
                }
                break;
            }
        }
        Ok(result)
    }
}
