use serde_json::Value;

pub struct QueryTranslator {}

impl QueryTranslator {
    pub fn query_document(&self, query: &serde_json::Value) -> Result<String, String> {
        let mut term_count = 0;

        let mut result = String::new();

        if let Some(query_doc) = query.as_object() {
            for (key, value) in query_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$or" => {
                            if value.is_object() {
                                if let Ok(res) = self.or(value) {
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
                            if value.is_object() {
                                if let Ok(res) = self.and(value) {
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
                                if let Ok(res) = self.not(value) {
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
                            if value.is_object() {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(", ");
                                    }

                                    in_values.push_str(self.value(val).unwrap().as_str());
                                }

                                result.push_str(&format!("( json_field('{}', raw) NOT IN {})", key, &in_values));
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
                        if let Ok(res) = self.nested(key, value) {
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

                        result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value).unwrap()));
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
        } else {
            return Err("Query is not an object".to_string());
        }

        Ok(result)
    }

    fn value(&self, value: &serde_json::Value) -> Result<String, String> {
        if value.is_string() {
            Ok(format!("'{}'", value.as_str().unwrap()))
        } else if value.is_number() {
            Ok(format!("{}", value.as_f64().unwrap()))
        } else if value.is_boolean() {
            Ok(format!("{}", value.as_bool().unwrap()))
        } else if value.is_f64() {
            Ok(format!("{}", value.as_f64().unwrap()))
        } else if value.is_i64() {
            Ok(format!("{}", value.as_i64().unwrap()))
        } else if value.is_u64() {
            Ok(format!("{}", value.as_u64().unwrap()))
        } else if value.is_null() {
            Ok("NULL".to_string())
        } else {
            Err(format!("Unsupported type: {}", value))
        }
    }

    fn nested(&self, scope: &str, value: &serde_json::Value) -> Result<String, String> {
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

                                return Ok(format!("json_field('{}', raw) < {}", scope, value));
                            } else {
                                return Err(format!("Error in $lt: {}", value));
                            }
                        }
                        "$gt" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $gt: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) > {}", scope, value));
                            } else {
                                return Err(format!("Error in $gt: {}", value));
                            }
                        }
                        "$gte" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $gte: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) >= {}", scope, value));
                            } else {
                                return Err(format!("Error in $gte: {}", value));
                            }
                        }
                        "$eq" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $eq: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) = {}", scope, value));
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

                                    in_values.push_str(self.value(val).unwrap().as_str());
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

                                return Ok(format!("json_field('{}', raw) <= {}", scope, value));
                            } else {
                                return Err(format!("Error in $lte: {}", value));
                            }
                        }
                        "$ne" => {
                            if value.is_number() || value.is_string() {
                                if term_count > 0 {
                                    return Err(format!("Error in $ne: {}", value));
                                }

                                return Ok(format!("json_field('{}', raw) != {}", scope, value));
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

                                    in_values.push_str(self.value(val).unwrap().as_str());
                                }
                                return Ok(format!("json_field('{}', raw) NOT IN ({})", scope, in_values));
                            } else {
                                return Err(format!("Error in $nin: {}", value));
                            }
                        }
                        _ => {}
                    }
                } else {
                    if value.is_object() {
                        if let Ok(res) = self.nested(key, value) {
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

                        result.push_str(&format!("json_field('{}.{}', raw) = {}", scope, key, self.value(value).unwrap()));
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

    fn or(&self, value: &serde_json::Value) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        if let Some(value_doc) = value.as_object() {
            for (key, value) in value_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$or" => {
                            if value.is_object() {
                                if let Ok(res) = self.or(value) {
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
                            if value.is_object() {
                                if let Ok(res) = self.and(value) {
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
                                if let Ok(res) = self.not(value) {
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
                            if value.is_object() {
                                if term_count > 0 {
                                    result.push_str(" OR ");
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(", ");
                                    }

                                    in_values.push_str(self.value(val).unwrap().as_str());
                                }

                                result.push_str(&format!("( json_field('{}', raw) NOT IN {})", key, &in_values));
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
                        if let Ok(res) = self.nested(key, value) {
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

                        result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value).unwrap()));
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

    fn and(&self, value: &serde_json::Value) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        if let Some(value_doc) = value.as_object() {
            for (key, value) in value_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$or" => {
                            if value.is_object() {
                                if let Ok(res) = self.or(value) {
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
                            if value.is_object() {
                                if let Ok(res) = self.and(value) {
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
                                if let Ok(res) = self.not(value) {
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
                            if value.is_object() {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }

                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(", ");
                                    }

                                    in_values.push_str(self.value(val).unwrap().as_str());
                                }

                                result.push_str(&format!("( json_field('{}', raw) NOT IN {})", key, &in_values));
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
                        if let Ok(res) = self.nested(key, value) {
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

                        result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value).unwrap()));
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

    fn not(&self, value: &serde_json::Value) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        if let Some(value_doc) = value.as_object() {
            for (key, value) in value_doc.iter() {
                if key.chars().nth(0).unwrap() == '$' {
                    match key.as_str() {
                        "$or" => {
                            if value.is_object() {
                                if let Ok(res) = self.or(value) {
                                    result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $or: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $or: {}", value));
                            }
                        }
                        "$and" => {
                            if value.is_object() {
                                if let Ok(res) = self.and(value) {
                                    result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
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
                                if let Ok(res) = self.not(value) {
                                    result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in $not: {}", value));
                                }
                            } else {
                                return Err(format!("Error in $not: {}", value));
                            }
                        }
                        "$nor" => {
                            if value.is_object() {
                                let mut in_values = String::new();

                                for val in value.as_array().unwrap() {
                                    if in_values.len() > 0 {
                                        in_values.push_str(", ");
                                    }

                                    in_values.push_str(self.value(val).unwrap().as_str());
                                }

                                result.push_str(&format!("( json_field('{}', raw) IN {})", key, &in_values));
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
                        if let Ok(res) = self.nested(key, value) {
                            result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                            term_count += 1;
                        } else {
                            return Err(format!("Error in nested query: {}", value));
                        }
                    } else if value.is_array() {
                    } else if value.is_string() || value.is_number() || value.is_boolean() || value.is_f64() || value.is_i64() || value.is_u64() {
                        result.push_str(&format!("json_field('{}', raw) IS NOT {}", key, self.value(value).unwrap()));
                        term_count += 1;
                    } else if value.is_null() {
                        result.push_str(&format!("json_field('{}', raw) IS NOT NULL", key));
                        term_count += 1;
                    } else {
                        return Err(format!("Unsupported type: {}", value));
                    }
                }
            }
        }
        Ok(result)
    }
}
