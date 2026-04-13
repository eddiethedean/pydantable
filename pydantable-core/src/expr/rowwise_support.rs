//! String and calendar helpers for row-wise expression evaluation (no Polars engine).

use crate::expr::ir::LiteralValue;

pub(super) fn trim_matches_char_set(s: &str, pat: &str) -> String {
    s.trim_matches(|c| pat.chars().any(|m| m == c)).to_string()
}

pub(super) fn wire_str(v: &LiteralValue) -> Option<&str> {
    match v {
        LiteralValue::Str(s) | LiteralValue::EnumStr(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Proleptic Gregorian calendar from Python / Polars `Date` days since 1970-01-01.
pub(super) fn ordinal_to_ymd(ordinal: i32) -> (i32, u32, u32) {
    let a = ordinal + 32044;
    let b = (4 * a + 3) / 146_097;
    let c = a - (146_097 * b) / 4;
    let d = (4 * c + 3) / 1461;
    let e = c - (1461 * d) / 4;
    let m = (5 * e + 2) / 153;
    let day = e - (153 * m + 2) / 5 + 1;
    let month = m + 3 - 12 * (m / 10);
    let year = b * 100 + d - 4800 + m / 10;
    (year, month as u32, day as u32)
}

pub(super) fn utc_calendar_from_epoch_days(days: i32) -> (i32, u32, u32) {
    ordinal_to_ymd(days + 719_163)
}

/// UTC wall time from Unix epoch microseconds (matches naive `datetime.fromtimestamp` semantics).
pub(super) fn utc_ymdhms_from_unix_micros(us: i64) -> (i32, u32, u32, u32, u32, u32) {
    let secs = us.div_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, mo, d) = utc_calendar_from_epoch_days(days as i32);
    let h = (sod / 3600) as u32;
    let mi = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    (y, mo, d, h, mi, s)
}

/// ISO weekday: Monday = 1, Sunday = 7 (matches Polars `dt.weekday()`).
pub(super) fn iso_weekday_monday1(y: i32, month: i32, day: i32) -> i64 {
    let mut y = y;
    let mut m = month;
    if m < 3 {
        m += 12;
        y -= 1;
    }
    let t = [0i32, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
    let w = (day + t[(m - 1) as usize] + y + y / 4 - y / 100 + y / 400) % 7;
    (((w + 6).rem_euclid(7)) + 1) as i64
}

/// ISO 8601 week number 1–53 (matches Polars `dt.week()` / Python `date.isocalendar().week`).
pub(super) fn iso_week_from_ymd(y: i32, month: u32, day: u32) -> i64 {
    use chrono::Datelike;
    chrono::NaiveDate::from_ymd_opt(y, month, day)
        .map(|d| d.iso_week().week() as i64)
        .unwrap_or(1)
}

/// Calendar day-of-year 1–366 (matches Polars `dt.ordinal_day()` / Spark `dayofyear`).
pub(super) fn ordinal_day_from_ymd(y: i32, month: u32, day: u32) -> i64 {
    use chrono::Datelike;
    chrono::NaiveDate::from_ymd_opt(y, month, day)
        .map(|d| i64::from(d.ordinal()))
        .unwrap_or(1)
}

pub(super) fn str_pad_start_chars(s: &str, length: u32, ch: char) -> String {
    let n = length as usize;
    let wc = s.chars().count();
    if wc >= n {
        return s.to_string();
    }
    std::iter::repeat(ch).take(n - wc).collect::<String>() + s
}

pub(super) fn str_pad_end_chars(s: &str, length: u32, ch: char) -> String {
    let n = length as usize;
    let wc = s.chars().count();
    if wc >= n {
        return s.to_string();
    }
    let mut out = s.to_string();
    out.extend(std::iter::repeat(ch).take(n - wc));
    out
}

pub(super) fn str_zfill_chars(s: &str, length: u32) -> String {
    let neg = s.starts_with('-');
    let body = if neg { &s[1..] } else { s };
    let n = length as usize;
    let body_w = body.chars().count();
    let total = body_w + usize::from(neg);
    if total >= n {
        return s.to_string();
    }
    let pad = n - total;
    let zeros: String = std::iter::repeat('0').take(pad).collect();
    if neg {
        format!("-{zeros}{body}")
    } else {
        format!("{zeros}{body}")
    }
}

pub(super) fn literal_between_inclusive(
    x: &LiteralValue,
    lo: &LiteralValue,
    hi: &LiteralValue,
) -> bool {
    if let (Some(xw), Some(lw), Some(hw)) = (wire_str(x), wire_str(lo), wire_str(hi)) {
        return xw >= lw && xw <= hw;
    }
    match (x, lo, hi) {
        (LiteralValue::Int(a), LiteralValue::Int(b), LiteralValue::Int(c)) => *a >= *b && *a <= *c,
        (LiteralValue::Float(a), LiteralValue::Float(b), LiteralValue::Float(c)) => {
            *a >= *b && *a <= *c
        }
        (LiteralValue::Int(a), LiteralValue::Int(b), LiteralValue::Float(c)) => {
            *a as f64 >= *b as f64 && *a as f64 <= *c
        }
        (LiteralValue::Int(a), LiteralValue::Float(b), LiteralValue::Float(c)) => {
            *a as f64 >= *b && *a as f64 <= *c
        }
        (LiteralValue::Float(a), LiteralValue::Int(b), LiteralValue::Int(c)) => {
            *a >= *b as f64 && *a <= *c as f64
        }
        (LiteralValue::Str(a), LiteralValue::Str(b), LiteralValue::Str(c)) => a >= b && a <= c,
        (LiteralValue::Uuid(a), LiteralValue::Uuid(b), LiteralValue::Uuid(c)) => a >= b && a <= c,
        (LiteralValue::Decimal(a), LiteralValue::Decimal(b), LiteralValue::Decimal(c)) => {
            a >= b && a <= c
        }
        _ => false,
    }
}
