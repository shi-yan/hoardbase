use bson::Bson;
use bson::Document;

pub struct QueryTranslator {}

impl QueryTranslator {
    pub fn query_document(&self, query: &bson::Document, params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        let mut term_count = 0;

        let mut result = String::new();

        for (key, value) in query.iter() {
            if key.chars().nth(0).unwrap() == '$' {
                match key.as_str() {
                    "$or" => {
                        if let bson::Bson::Array(arr) = value {
                            if let Ok(res) = self.or(arr, params) {
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
                        if let bson::Bson::Array(arr) = value {
                            if let Ok(res) = self.and(arr, params) {
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
                        if let bson::Bson::Document(doc) = value {
                            if let Ok(res) = self.not(doc, params) {
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
                        if let bson::Bson::Array(arr) = value {
                            if term_count > 0 {
                                result.push_str(" AND ");
                            }

                            let mut in_values = String::new();

                            for val in arr {
                                if let bson::Bson::Document(val_doc) = val {
                                    if in_values.len() > 0 {
                                        in_values.push_str(" OR ");
                                    }

                                    in_values.push_str(self.nested("", val_doc, params).unwrap().as_str());
                                }
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
                match value {
                    bson::Bson::Document(val_doc) => {
                        if key == "_id" {
                            return Err(format!("_id cannot be object"));
                        } else if let Ok(res) = self.nested(key, &val_doc, params) {
                            if term_count > 0 {
                                result.push_str(" AND ");
                            }

                            result.push_str(&res);
                            term_count += 1;
                        } else {
                            return Err(format!("Error in nested query: {}", value));
                        }
                    }

                    bson::Bson::Null => {
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
                    }

                    bson::Bson::Array(arr) => {
                        return Err(format!("Unsupported type: {}", value));
                    }

                    bson::Bson::String(val) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        match key.as_str() {
                            "_id" => {
                                return Err(format!("_id cannot be string"));
                            }
                            _ => {
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                            }
                        }
                        term_count += 1;
                    }
                    bson::Bson::Int32(val) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        match key.as_str() {
                            "_id" => {
                                result.push_str(&format!("{} = '{}'", key, val));
                            }
                            _ => {
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                            }
                        }
                        term_count += 1;
                    }
                    bson::Bson::Int64(val) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        match key.as_str() {
                            "_id" => {
                                result.push_str(&format!("{} = '{}'", key, val));
                            }
                            _ => {
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                            }
                        }
                        term_count += 1;
                    }
                    bson::Bson::Double(val) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        match key.as_str() {
                            "_id" => {
                                return Err(format!("_id cannot be double"));
                            }
                            _ => {
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                            }
                        }
                        term_count += 1;
                    }
                    bson::Bson::Boolean(val) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        match key.as_str() {
                            "_id" => {
                                return Err(format!("_id cannot be boolean"));
                            }
                            _ => {
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                            }
                        }
                        term_count += 1;
                    }
                    _ => {
                        return Err(format!("Unsupported type: {}", value));
                    }
                }
            }
        }

        Ok(result)
    }

    fn value(&self, value: &bson::Bson, params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        match value {
            bson::Bson::String(val) => {
                params.push(rusqlite::types::Value::from((*val).clone()));
            }
            bson::Bson::Int32(val) => {
                params.push(rusqlite::types::Value::from(*val));
            }
            bson::Bson::Boolean(val) => {
                params.push(rusqlite::types::Value::from(*val));
            }
            bson::Bson::Double(val) => {
                params.push(rusqlite::types::Value::from(*val));
            }
            bson::Bson::Int64(val) => {
                params.push(rusqlite::types::Value::from(*val));
            }
            bson::Bson::Null => {
                params.push(rusqlite::types::Value::Null);
            }
            _ => {
                return Err(format!("Unsupported type: {}", value));
            }
        }

        Ok(format!("?{}", params.len()))
    }

    fn nested(&self, scope: &str, value_doc: &bson::Document, params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        for (key, value) in value_doc.iter() {
            if key.chars().nth(0).unwrap() == '$' {
                match key.as_str() {
                    "$lt" => match value {
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $lt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) < {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $lt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) < {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Double(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $lt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) < {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::String(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $lt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) < {}", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $lt: {}", value));
                        }
                    },
                    "$gt" => match value {
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) > {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) > {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Double(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) > {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::String(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) > {}", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $gt: {}", value));
                        }
                    },
                    "$gte" => match value {
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) >= {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) >= {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Double(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) >= {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::String(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) >= {}", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $gt: {}", value));
                        }
                    },
                    "$eq" => match value {
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Double(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::String(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $gt: {}", value));
                        }
                    },
                    "$in" => {
                        if let bson::Bson::Array(arr) = value {
                            if term_count > 0 {
                                return Err(format!("Error in $in: {}", value));
                            }

                            let mut in_values = String::new();

                            for val in arr {
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
                    "$lte" => match value {
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) <= {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) <= {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Double(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) <= {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::String(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) <= {}", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $gt: {}", value));
                        }
                    },
                    "$ne" => match value {
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) != {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) != {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Double(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) != {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::String(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $gt: {}", value));
                            }

                            return Ok(format!("json_field('{}', raw) != {}", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $gt: {}", value));
                        }
                    },
                    "$nin" => {
                        if let bson::Bson::Array(arr) = value {
                            if term_count > 0 {
                                return Err(format!("Error in $nin: {}", value));
                            }

                            let mut in_values = String::new();

                            for val in arr {
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
                        if let bson::Bson::Boolean(val) = value {
                            if term_count > 0 {
                                return Err(format!("Error in $exists: {}", value));
                            }

                            return Ok(format!("json_field_exists('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        } else {
                            return Err(format!("Error in $exists: {}", value));
                        }
                    }
                    "$type" => match value {
                        bson::Bson::String(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $exists: {}", value));
                            }

                            return Ok(format!("json_field_type('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Array(arr) => {
                            if term_count > 0 {
                                return Err(format!("Error in $exists: {}", value));
                            }

                            let mut in_values = String::new();

                            for val in arr {
                                if let bson::Bson::String(val_str) = val {
                                    if in_values.len() > 0 {
                                        in_values.push_str(", ");
                                    }

                                    in_values.push_str(self.value(val, params).unwrap().as_str());
                                } else {
                                    return Err(format!("Error in $exists: {}", value));
                                }
                            }

                            return Ok(format!("json_field_type('{}', raw) IN ({})", scope, in_values));
                        }
                        _ => {
                            return Err(format!("Error in $exists: {}", value));
                        }
                    },
                    "$size" => match value {
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $size: {}", value));
                            }

                            return Ok(format!("json_field_size('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $size: {}", value));
                            }

                            return Ok(format!("json_field_size('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Double(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $size: {}", value));
                            }

                            return Ok(format!("json_field_size('{}', raw) = {}", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $size: {}", value));
                        }
                    },
                    "$all" => {
                        if let bson::Bson::Array(arr) = value {
                            if term_count > 0 {
                                return Err(format!("Error in $all: {}", value));
                            }

                            let mut in_values = String::new();

                            for val in arr {
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
                        if let bson::Bson::Array(arr) = value {
                            if term_count > 0 {
                                return Err(format!("Error in $elemMatch: {}", value));
                            }

                            let mut in_values = String::new();

                            for val in arr {
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
                    "$bitsAllClear" => match value {
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $bitsAllClear: {}", value));
                            }

                            return Ok(format!("json_field_bits_all_clear('{}', raw, {})", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $bitsAllClear: {}", value));
                            }

                            return Ok(format!("json_field_bits_all_clear('{}', raw, {})", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $bitsAllClear: {}", value));
                        }
                    },
                    "$bitsAllSet" => match value {
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $bitsAllSet: {}", value));
                            }

                            return Ok(format!("json_field_bits_all_set('{}', raw, {})", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $bitsAllSet: {}", value));
                            }

                            return Ok(format!("json_field_bits_all_set('{}', raw, {})", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $bitsAllSet: {}", value));
                        }
                    },
                    "$bitsAnyClear" => match value {
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $bitsAnyClear: {}", value));
                            }

                            return Ok(format!("json_field_bits_any_clear('{}', raw, {})", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $bitsAnyClear: {}", value));
                            }

                            return Ok(format!("json_field_bits_any_clear('{}', raw, {})", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $bitsAnyClear: {}", value));
                        }
                    },
                    "$bitsAnySet" => match value {
                        bson::Bson::Int64(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $bitsAnySet: {}", value));
                            }

                            return Ok(format!("json_field_bits_any_set('{}', raw, {})", scope, self.value(value, params).unwrap()));
                        }
                        bson::Bson::Int32(val) => {
                            if term_count > 0 {
                                return Err(format!("Error in $bitsAnySet: {}", value));
                            }

                            return Ok(format!("json_field_bits_any_set('{}', raw, {})", scope, self.value(value, params).unwrap()));
                        }
                        _ => {
                            return Err(format!("Error in $bitsAnySet: {}", value));
                        }
                    },
                    "$mod" => {
                        if let bson::Bson::Array(arr) = value {
                            if arr.len() != 2 {
                                return Err(format!("Error in $mod: {}", value));
                            }

                            let divisor = arr[0].clone();
                            let remainder = arr[1].clone();

                            if divisor.as_i64().unwrap() == 0 {
                                return Err(format!("Error in $mod: {}, Divisor can't be zero.", value));
                            }

                            return Ok(format!("json_field('{}', raw) % {} = {}", scope, self.value(&divisor, params).unwrap(), self.value(&remainder, params).unwrap()));
                        } else {
                            return Err(format!("Error in $mod: {}", value));
                        }
                    }
                    //todo $jsonSchema and $text not implemented
                    "$regex" => {
                        if let bson::Bson::String(_) = value {
                            let mut options = String::new();
                            if value_doc.keys().count() > 1 {
                                if let Some(option_obj) = value_doc.get("$options") {
                                    if let bson::Bson::String(str_val) = option_obj {
                                        options = str_val.to_string();
                                    } else {
                                        return Err(format!("Error in $regex: {}", value));
                                    }
                                }
                            }

                            let regex = value;

                            return Ok(format!("json_field_regex('{}', raw, {}, {})", scope, self.value(regex, params).unwrap(), self.value(&bson::Bson::String(options), params).unwrap()));
                        } else {
                            return Err(format!("Error in $regex: {}", value));
                        }
                    }
                    _ => {}
                }
            } else {
                match value {
                    bson::Bson::Document(value_doc) => {
                        if let Ok(res) = self.nested(key, &value_doc, params) {
                            if term_count > 0 {
                                result.push_str(" AND ");
                            }
                            result.push_str(&res);
                            term_count += 1;
                        } else {
                            return Err(format!("Error in nested query: {}", value_doc));
                        }
                    }

                    bson::Bson::Array(arr) => {
                        return Err(format!("Error in array: {:?}", arr));
                    }

                    bson::Bson::String(value_str) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        result.push_str(&format!("json_field('{}.{}', raw) = {}", scope, key, self.value(value, params).unwrap()));
                        term_count += 1;
                    }

                    bson::Bson::Int32(value_i32) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        result.push_str(&format!("json_field('{}.{}', raw) = {}", scope, key, self.value(value, params).unwrap()));
                        term_count += 1;
                    }

                    bson::Bson::Int64(value_i64) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        result.push_str(&format!("json_field('{}.{}', raw) = {}", scope, key, self.value(value, params).unwrap()));
                        term_count += 1;
                    }

                    bson::Bson::Double(value_f64) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        result.push_str(&format!("json_field('{}.{}', raw) = {}", scope, key, self.value(value, params).unwrap()));
                        term_count += 1;
                    }

                    bson::Bson::Boolean(value_bool) => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        result.push_str(&format!("json_field('{}.{}', raw) = {}", scope, key, self.value(value, params).unwrap()));
                        term_count += 1;
                    }

                    bson::Bson::Null => {
                        if term_count > 0 {
                            result.push_str(" AND ");
                        }
                        result.push_str(&format!("json_field('{}.{}', raw) IS NULL", scope, key));
                        term_count += 1;
                    }

                    _ => {
                        return Err(format!("Error in value: {:?}", value));
                    }
                }
            }
        }

        Ok(result)
    }

    fn or(&self, arr: &bson::Array, params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        for doc in arr {
            if let bson::Bson::Document(value_doc) = doc {
                for (key, value) in value_doc.iter() {
                    if key.chars().nth(0).unwrap() == '$' {
                        match key.as_str() {
                            "$or" => {
                                if let bson::Bson::Array(arr) = value {
                                    if let Ok(res) = self.or(arr, params) {
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
                                if let bson::Bson::Array(arr) = value {
                                    if let Ok(res) = self.and(arr, params) {
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
                                if let bson::Bson::Document(value_doc) = value {
                                    if let Ok(res) = self.not(value_doc, params) {
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
                                if let bson::Bson::Array(arr) = value {
                                    if term_count > 0 {
                                        result.push_str(" OR ");
                                    }

                                    let mut in_values = String::new();

                                    for val in arr {
                                        if let bson::Bson::Document(value_doc) = val {
                                            if in_values.len() > 0 {
                                                in_values.push_str(" OR ");
                                            }

                                            in_values.push_str(self.nested("", value_doc, params).unwrap().as_str());
                                        } else {
                                            return Err(format!("Error in $nor: {}", value));
                                        }
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
                        match value {
                            bson::Bson::String(val) => {
                                if term_count > 0 {
                                    result.push_str(" OR ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Int64(val) => {
                                if term_count > 0 {
                                    result.push_str(" OR ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Int32(val) => {
                                if term_count > 0 {
                                    result.push_str(" OR ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Double(val) => {
                                if term_count > 0 {
                                    result.push_str(" OR ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Boolean(val) => {
                                if term_count > 0 {
                                    result.push_str(" OR ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Document(value_doc) => {
                                if let Ok(res) = self.nested(key, &value_doc, params) {
                                    if term_count > 0 {
                                        result.push_str(" OR ");
                                    }
                                    result.push_str(&res);
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in nested query: {}", value));
                                }
                            }
                            bson::Bson::Array(arr) => {
                                return Err(format!("Error in array: {}", value));
                            }
                            bson::Bson::Null => {
                                if term_count > 0 {
                                    result.push_str(" OR ");
                                }
                                result.push_str(&format!("json_field('{}', raw) IS NULL", key));
                                term_count += 1;
                            }
                            _ => {
                                return Err(format!("Unsupported type: {}", value));
                            }
                        }
                    }
                }
            } else {
                return Err(format!("Unsupported type"));
            }
        }
        Ok(result)
    }

    fn and(&self, arr: &bson::Array, params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        let mut result = String::new();
        let mut term_count = 0;
        for doc in arr {
            if let bson::Bson::Document(value_doc) = doc {
                for (key, value) in value_doc.iter() {
                    if key.chars().nth(0).unwrap() == '$' {
                        match key.as_str() {
                            "$or" => {
                                if let bson::Bson::Array(arr) = value {
                                    if let Ok(res) = self.or(arr, params) {
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
                                if let bson::Bson::Array(arr) = value {
                                    if let Ok(res) = self.and(arr, params) {
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
                                if let bson::Bson::Document(value_doc) = value {
                                    if let Ok(res) = self.not(&value_doc, params) {
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
                                if let bson::Bson::Array(arr) = value {
                                    if term_count > 0 {
                                        result.push_str(" AND ");
                                    }

                                    let mut in_values = String::new();

                                    for val in arr {
                                        if let bson::Bson::Document(doc) = val {
                                            if in_values.len() > 0 {
                                                in_values.push_str(" OR ");
                                            }

                                            in_values.push_str(self.nested("", &doc, params).unwrap().as_str());
                                        }
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
                        match value {
                            bson::Bson::Document(doc) => {
                                if let Ok(res) = self.nested(key, &doc, params) {
                                    if term_count > 0 {
                                        result.push_str(" AND ");
                                    }
                                    result.push_str(&res);
                                    term_count += 1;
                                } else {
                                    return Err(format!("Error in nested query: {}", value));
                                }
                            }
                            bson::Bson::Array(arr) => {
                                return Err(format!("Error in array: {}", value));
                            }
                            bson::Bson::String(val) => {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Int32(val) => {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Int64(val) => {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Boolean(val) => {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Double(val) => {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }
                                result.push_str(&format!("json_field('{}', raw) = {}", key, self.value(value, params).unwrap()));
                                term_count += 1;
                            }
                            bson::Bson::Null => {
                                if term_count > 0 {
                                    result.push_str(" AND ");
                                }
                                result.push_str(&format!("json_field('{}', raw) IS NULL", key));
                                term_count += 1;
                            }
                            _ => {
                                return Err(format!("Unsupported value: {}", value));
                            }
                        }
                    }
                }
            } else {
                return Err(format!("Unsupported type"));
            }
        }
        Ok(result)
    }

    fn not(&self, value_doc: &bson::Document, params: &mut Vec<rusqlite::types::Value>) -> Result<String, String> {
        let mut result = String::new();
        for (key, value) in value_doc.iter() {
            if key.chars().nth(0).unwrap() == '$' {
                match key.as_str() {
                    "$or" => {
                        if let bson::Bson::Array(arr) = value {
                            if let Ok(res) = self.or(arr, params) {
                                result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                            } else {
                                return Err(format!("Error in $or: {}", value));
                            }
                        } else {
                            return Err(format!("Error in $or: {}", value));
                        }
                    }
                    "$and" => {
                        if let bson::Bson::Array(arr) = value {
                            if let Ok(res) = self.and(arr, params) {
                                result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                            } else {
                                return Err(format!("Error in $and: {}", value));
                            }
                        } else {
                            return Err(format!("Error in $and: {}", value));
                        }
                    }
                    "$not" => {
                        if let bson::Bson::Document(val_doc) = value {
                            if let Ok(res) = self.not(val_doc, params) {
                                result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                            } else {
                                return Err(format!("Error in $not: {}", value));
                            }
                        } else {
                            return Err(format!("Error in $not: {}", value));
                        }
                    }
                    "$nor" => {
                        if let bson::Bson::Array(arr) = value {
                            let mut in_values = String::new();

                            for val in arr {
                                if let bson::Bson::Document(doc) = val {
                                    if in_values.len() > 0 {
                                        in_values.push_str(" OR ");
                                    }

                                    in_values.push_str(self.nested("", &doc, params).unwrap().as_str());
                                } else {
                                    return Err(format!("Error in $nor: {}", value));
                                }
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
                match value {
                    bson::Bson::Document(doc) => {
                        if let Ok(res) = self.nested(key, &doc, params) {
                            result.push_str(&format!("json_field('{}', raw) IS NOT ({})", key, &res));
                        } else {
                            return Err(format!("Error in nested query: {}", value));
                        }
                    }
                    bson::Bson::Array(arr) => {
                        return Err(format!("Unsupported type: {}", value));
                    }
                    bson::Bson::String(val) => {
                        result.push_str(&format!("json_field('{}', raw) IS NOT {}", key, self.value(value, params).unwrap()));
                    }
                    bson::Bson::Boolean(val) => {
                        result.push_str(&format!("json_field('{}', raw) IS NOT {}", key, self.value(value, params).unwrap()));
                    }
                    bson::Bson::Int64(val) => {
                        result.push_str(&format!("json_field('{}', raw) IS NOT {}", key, self.value(value, params).unwrap()));
                    }
                    bson::Bson::Int32(val) => {
                        result.push_str(&format!("json_field('{}', raw) IS NOT {}", key, self.value(value, params).unwrap()));
                    }
                    bson::Bson::Double(val) => {
                        result.push_str(&format!("json_field('{}', raw) IS NOT {}", key, self.value(value, params).unwrap()));
                    }
                    bson::Bson::Null => {
                        result.push_str(&format!("json_field('{}', raw) IS NOT NULL", key));
                    }
                    _ => {
                        return Err(format!("Unsupported type: {}", value));
                    }
                }
            }
            break;
        }
        Ok(result)
    }
}
