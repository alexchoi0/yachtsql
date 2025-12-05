#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_i64_avx2(data: &[i64]) -> i64 {
    let mut sum_vec = _mm256_setzero_si256();
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
            sum_vec = _mm256_add_epi64(sum_vec, v);
        }
    }

    let mut tmp = [0i64; 4];
    unsafe {
        _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, sum_vec);
    }
    let sum = tmp.iter().sum::<i64>();

    let remainder_sum: i64 = remainder.iter().sum();
    sum + remainder_sum
}

#[cfg(target_arch = "x86_64")]
pub fn sum_i64(data: &[i64]) -> i64 {
    if data.is_empty() {
        return 0;
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { sum_i64_avx2(data) }
    } else {
        data.iter().sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_f64_avx2(data: &[f64]) -> f64 {
    let mut sum_vec = _mm256_setzero_pd();
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_pd(chunk.as_ptr());
            sum_vec = _mm256_add_pd(sum_vec, v);
        }
    }

    let mut tmp = [0.0f64; 4];
    unsafe {
        _mm256_storeu_pd(tmp.as_mut_ptr(), sum_vec);
    }
    let sum = tmp.iter().sum::<f64>();

    let remainder_sum: f64 = remainder.iter().sum();
    sum + remainder_sum
}

#[cfg(target_arch = "x86_64")]
pub fn sum_f64(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { sum_f64_avx2(data) }
    } else {
        data.iter().sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_i64_masked_avx2(data: &[i64], nulls: &[u8]) -> i64 {
    let mut sum_vec = _mm256_setzero_si256();
    let chunks_data = data.chunks_exact(4);
    let chunks_nulls = nulls.chunks_exact(4);
    let remainder_data = chunks_data.remainder();
    let remainder_nulls = chunks_nulls.remainder();

    for (chunk_data, chunk_nulls) in chunks_data.zip(chunks_nulls) {
        unsafe {
            let data_vec = _mm256_loadu_si256(chunk_data.as_ptr() as *const __m256i);

            let mask_0 = if chunk_nulls[0] != 0 { -1i64 } else { 0i64 };
            let mask_1 = if chunk_nulls[1] != 0 { -1i64 } else { 0i64 };
            let mask_2 = if chunk_nulls[2] != 0 { -1i64 } else { 0i64 };
            let mask_3 = if chunk_nulls[3] != 0 { -1i64 } else { 0i64 };
            let mask_vec = _mm256_set_epi64x(mask_3, mask_2, mask_1, mask_0);

            let masked = _mm256_and_si256(data_vec, mask_vec);

            sum_vec = _mm256_add_epi64(sum_vec, masked);
        }
    }

    let mut tmp = [0i64; 4];
    unsafe {
        _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, sum_vec);
    }
    let sum = tmp.iter().sum::<i64>();

    let remainder_sum: i64 = remainder_data
        .iter()
        .zip(remainder_nulls.iter())
        .filter(|&(_, &n)| n != 0)
        .map(|(&d, _)| d)
        .sum();

    sum + remainder_sum
}

#[cfg(target_arch = "x86_64")]
pub fn sum_i64_masked(data: &[i64], nulls: &[u8]) -> i64 {
    assert_eq!(data.len(), nulls.len());

    if data.is_empty() {
        return 0;
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { sum_i64_masked_avx2(data, nulls) }
    } else {
        data.iter()
            .zip(nulls.iter())
            .filter(|&(_, &n)| n != 0)
            .map(|(&d, _)| d)
            .sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_f64_masked_avx2(data: &[f64], nulls: &[u8]) -> f64 {
    let mut sum_vec = _mm256_setzero_pd();
    let chunks_data = data.chunks_exact(4);
    let chunks_nulls = nulls.chunks_exact(4);
    let remainder_data = chunks_data.remainder();
    let remainder_nulls = chunks_nulls.remainder();

    for (chunk_data, chunk_nulls) in chunks_data.zip(chunks_nulls) {
        unsafe {
            let data_vec = _mm256_loadu_pd(chunk_data.as_ptr());

            let mask_0: i64 = if chunk_nulls[0] != 0 { -1 } else { 0 };
            let mask_1: i64 = if chunk_nulls[1] != 0 { -1 } else { 0 };
            let mask_2: i64 = if chunk_nulls[2] != 0 { -1 } else { 0 };
            let mask_3: i64 = if chunk_nulls[3] != 0 { -1 } else { 0 };
            let mask_vec = _mm256_castsi256_pd(_mm256_set_epi64x(mask_3, mask_2, mask_1, mask_0));

            let masked = _mm256_and_pd(data_vec, mask_vec);

            sum_vec = _mm256_add_pd(sum_vec, masked);
        }
    }

    let mut tmp = [0.0f64; 4];
    unsafe {
        _mm256_storeu_pd(tmp.as_mut_ptr(), sum_vec);
    }
    let sum = tmp.iter().sum::<f64>();

    let remainder_sum: f64 = remainder_data
        .iter()
        .zip(remainder_nulls.iter())
        .filter(|&(_, &n)| n != 0)
        .map(|(&d, _)| d)
        .sum();

    sum + remainder_sum
}

#[cfg(target_arch = "x86_64")]
pub fn sum_f64_masked(data: &[f64], nulls: &[u8]) -> f64 {
    assert_eq!(data.len(), nulls.len());

    if data.is_empty() {
        return 0.0;
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { sum_f64_masked_avx2(data, nulls) }
    } else {
        data.iter()
            .zip(nulls.iter())
            .filter(|&(_, &n)| n != 0)
            .map(|(&d, _)| d)
            .sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn count_nonzero_i64_avx2(data: &[i64]) -> usize {
    let zero_vec = _mm256_setzero_si256();
    let mut count = 0usize;
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

            let cmp = _mm256_cmpeq_epi64(v, zero_vec);

            let mask = _mm256_movemask_epi8(cmp);

            if (mask & 0x000000FF) != 0xFF {
                count += 1;
            }
            if (mask & 0x0000FF00) != 0xFF00 {
                count += 1;
            }
            if (mask & 0x00FF0000) != 0xFF0000 {
                count += 1;
            }
            if (mask as u32 & 0xFF000000) != 0xFF000000 {
                count += 1;
            }
        }
    }

    count += remainder.iter().filter(|&&x| x != 0).count();
    count
}

#[cfg(target_arch = "x86_64")]
pub fn count_nonzero_i64(data: &[i64]) -> usize {
    if data.is_empty() {
        return 0;
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { count_nonzero_i64_avx2(data) }
    } else {
        data.iter().filter(|&&x| x != 0).count()
    }
}

#[cfg(target_arch = "x86_64")]
pub fn count_nonzero_u8(data: &[u8]) -> usize {
    data.iter().filter(|&&x| x != 0).count()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_gt_i64_avx2(data: &[i64], threshold: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    let threshold_vec = _mm256_set1_epi64x(threshold);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut idx = 0;

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

            let cmp = _mm256_cmpgt_epi64(v, threshold_vec);
            let mask = _mm256_movemask_epi8(cmp);

            if (mask & 0x000000FF) == 0xFF {
                indices.push(idx);
            }
            if (mask & 0x0000FF00) == 0xFF00 {
                indices.push(idx + 1);
            }
            if (mask & 0x00FF0000) == 0xFF0000 {
                indices.push(idx + 2);
            }
            if (mask as u32 & 0xFF000000) == 0xFF000000 {
                indices.push(idx + 3);
            }
        }

        idx += 4;
    }

    for (i, &value) in remainder.iter().enumerate() {
        if value > threshold {
            indices.push(idx + i);
        }
    }

    indices
}

#[cfg(target_arch = "x86_64")]
pub fn filter_gt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { filter_gt_i64_avx2(data, threshold) }
    } else {
        data.iter()
            .enumerate()
            .filter(|&(_, &v)| v > threshold)
            .map(|(i, _)| i)
            .collect()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_gt_i64_with_nulls_avx2(data: &[i64], nulls: &[u8], threshold: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    let threshold_vec = _mm256_set1_epi64x(threshold);
    let chunks_data = data.chunks_exact(4);
    let chunks_nulls = nulls.chunks_exact(4);
    let remainder_data = chunks_data.remainder();
    let remainder_nulls = chunks_nulls.remainder();
    let mut idx = 0;

    for (chunk_data, chunk_nulls) in chunks_data.zip(chunks_nulls) {
        unsafe {
            let v = _mm256_loadu_si256(chunk_data.as_ptr() as *const __m256i);

            let cmp_gt = _mm256_cmpgt_epi64(v, threshold_vec);

            let null0 = if chunk_nulls[0] != 0 { -1i64 } else { 0i64 };
            let null1 = if chunk_nulls[1] != 0 { -1i64 } else { 0i64 };
            let null2 = if chunk_nulls[2] != 0 { -1i64 } else { 0i64 };
            let null3 = if chunk_nulls[3] != 0 { -1i64 } else { 0i64 };
            let null_mask = _mm256_set_epi64x(null3, null2, null1, null0);

            let combined = _mm256_and_si256(cmp_gt, null_mask);
            let mask = _mm256_movemask_epi8(combined);

            if (mask & 0x000000FF) == 0xFF {
                indices.push(idx);
            }
            if (mask & 0x0000FF00) == 0xFF00 {
                indices.push(idx + 1);
            }
            if (mask & 0x00FF0000) == 0xFF0000 {
                indices.push(idx + 2);
            }
            if (mask as u32 & 0xFF000000) == 0xFF000000 {
                indices.push(idx + 3);
            }
        }

        idx += 4;
    }

    for (i, (&value, &null_flag)) in remainder_data
        .iter()
        .zip(remainder_nulls.iter())
        .enumerate()
    {
        if value > threshold && null_flag != 0 {
            indices.push(idx + i);
        }
    }

    indices
}

#[cfg(target_arch = "x86_64")]
pub fn filter_gt_i64_with_nulls(data: &[i64], nulls: &[u8], threshold: i64) -> Vec<usize> {
    assert_eq!(data.len(), nulls.len());
    if data.is_empty() {
        return Vec::new();
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { filter_gt_i64_with_nulls_avx2(data, nulls, threshold) }
    } else {
        data.iter()
            .zip(nulls.iter())
            .enumerate()
            .filter(|&(_, (&v, &null_flag))| v > threshold && null_flag != 0)
            .map(|(i, _)| i)
            .collect()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_gt_f64_avx2(data: &[f64], threshold: f64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    let threshold_vec = _mm256_set1_pd(threshold);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut idx = 0;

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_pd(chunk.as_ptr());
            let cmp = _mm256_cmp_pd(v, threshold_vec, _CMP_GT_OQ);
            let mask = _mm256_movemask_pd(cmp);

            if (mask & 0x1) != 0 {
                indices.push(idx);
            }
            if (mask & 0x2) != 0 {
                indices.push(idx + 1);
            }
            if (mask & 0x4) != 0 {
                indices.push(idx + 2);
            }
            if (mask & 0x8) != 0 {
                indices.push(idx + 3);
            }
        }

        idx += 4;
    }

    for (i, &value) in remainder.iter().enumerate() {
        if value > threshold {
            indices.push(idx + i);
        }
    }

    indices
}

#[cfg(target_arch = "x86_64")]
pub fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { filter_gt_f64_avx2(data, threshold) }
    } else {
        data.iter()
            .enumerate()
            .filter(|&(_, &v)| v > threshold)
            .map(|(i, _)| i)
            .collect()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn min_i64_avx2(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }

    let mut min_vec = _mm256_set1_epi64x(i64::MAX);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
            let cmp = _mm256_cmpgt_epi64(min_vec, v);
            min_vec = _mm256_blendv_epi8(min_vec, v, cmp);
        }
    }

    let mut tmp = [0i64; 4];
    unsafe {
        _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, min_vec);
    }
    let min_val = tmp.iter().min().copied().unwrap();

    let remainder_min = remainder.iter().min().copied().unwrap_or(i64::MAX);
    Some(min_val.min(remainder_min))
}

#[cfg(target_arch = "x86_64")]
pub fn min_i64(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { min_i64_avx2(data) }
    } else {
        data.iter().min().copied()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn max_i64_avx2(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }

    let mut max_vec = _mm256_set1_epi64x(i64::MIN);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
            let cmp = _mm256_cmpgt_epi64(v, max_vec);
            max_vec = _mm256_blendv_epi8(max_vec, v, cmp);
        }
    }

    let mut tmp = [0i64; 4];
    unsafe {
        _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, max_vec);
    }
    let max_val = tmp.iter().max().copied().unwrap();

    let remainder_max = remainder.iter().max().copied().unwrap_or(i64::MIN);
    Some(max_val.max(remainder_max))
}

#[cfg(target_arch = "x86_64")]
pub fn max_i64(data: &[i64]) -> Option<i64> {
    if data.is_empty() {
        return None;
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { max_i64_avx2(data) }
    } else {
        data.iter().max().copied()
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_eq_i64_avx2(data: &[i64], value: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    let value_vec = _mm256_set1_epi64x(value);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut idx = 0;

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
            let cmp = _mm256_cmpeq_epi64(v, value_vec);
            let mask = _mm256_movemask_epi8(cmp);

            if (mask & 0x000000FF) == 0xFF {
                indices.push(idx);
            }
            if (mask & 0x0000FF00) == 0xFF00 {
                indices.push(idx + 1);
            }
            if (mask & 0x00FF0000) == 0xFF0000 {
                indices.push(idx + 2);
            }
            if (mask as u32 & 0xFF000000) == 0xFF000000 {
                indices.push(idx + 3);
            }
        }

        idx += 4;
    }

    for (i, &v) in remainder.iter().enumerate() {
        if v == value {
            indices.push(idx + i);
        }
    }

    indices
}

#[cfg(target_arch = "x86_64")]
pub fn filter_eq_i64(data: &[i64], value: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { filter_eq_i64_avx2(data, value) }
    } else {
        super::scalar::filter_eq_i64(data, value)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_lt_i64_avx2(data: &[i64], threshold: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    let threshold_vec = _mm256_set1_epi64x(threshold);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut idx = 0;

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

            let cmp = _mm256_cmpgt_epi64(threshold_vec, v);
            let mask = _mm256_movemask_epi8(cmp);

            if (mask & 0x000000FF) == 0xFF {
                indices.push(idx);
            }
            if (mask & 0x0000FF00) == 0xFF00 {
                indices.push(idx + 1);
            }
            if (mask & 0x00FF0000) == 0xFF0000 {
                indices.push(idx + 2);
            }
            if (mask as u32 & 0xFF000000) == 0xFF000000 {
                indices.push(idx + 3);
            }
        }

        idx += 4;
    }

    for (i, &value) in remainder.iter().enumerate() {
        if value < threshold {
            indices.push(idx + i);
        }
    }

    indices
}

#[cfg(target_arch = "x86_64")]
pub fn filter_lt_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { filter_lt_i64_avx2(data, threshold) }
    } else {
        super::scalar::filter_lt_i64(data, threshold)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_gte_i64_avx2(data: &[i64], threshold: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    let threshold_vec = _mm256_set1_epi64x(threshold);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut idx = 0;

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

            let cmp_lt = _mm256_cmpgt_epi64(threshold_vec, v);
            let cmp_gte = _mm256_xor_si256(cmp_lt, _mm256_set1_epi64x(-1));
            let mask = _mm256_movemask_epi8(cmp_gte);

            if (mask & 0x000000FF) == 0xFF {
                indices.push(idx);
            }
            if (mask & 0x0000FF00) == 0xFF00 {
                indices.push(idx + 1);
            }
            if (mask & 0x00FF0000) == 0xFF0000 {
                indices.push(idx + 2);
            }
            if (mask as u32 & 0xFF000000) == 0xFF000000 {
                indices.push(idx + 3);
            }
        }

        idx += 4;
    }

    for (i, &value) in remainder.iter().enumerate() {
        if value >= threshold {
            indices.push(idx + i);
        }
    }

    indices
}

#[cfg(target_arch = "x86_64")]
pub fn filter_gte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { filter_gte_i64_avx2(data, threshold) }
    } else {
        super::scalar::filter_gte_i64(data, threshold)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_lte_i64_avx2(data: &[i64], threshold: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    let threshold_vec = _mm256_set1_epi64x(threshold);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut idx = 0;

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

            let cmp_gt = _mm256_cmpgt_epi64(v, threshold_vec);
            let cmp_lte = _mm256_xor_si256(cmp_gt, _mm256_set1_epi64x(-1));
            let mask = _mm256_movemask_epi8(cmp_lte);

            if (mask & 0x000000FF) == 0xFF {
                indices.push(idx);
            }
            if (mask & 0x0000FF00) == 0xFF00 {
                indices.push(idx + 1);
            }
            if (mask & 0x00FF0000) == 0xFF0000 {
                indices.push(idx + 2);
            }
            if (mask as u32 & 0xFF000000) == 0xFF000000 {
                indices.push(idx + 3);
            }
        }

        idx += 4;
    }

    for (i, &value) in remainder.iter().enumerate() {
        if value <= threshold {
            indices.push(idx + i);
        }
    }

    indices
}

#[cfg(target_arch = "x86_64")]
pub fn filter_lte_i64(data: &[i64], threshold: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { filter_lte_i64_avx2(data, threshold) }
    } else {
        super::scalar::filter_lte_i64(data, threshold)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_ne_i64_avx2(data: &[i64], value: i64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(data.len());
    let value_vec = _mm256_set1_epi64x(value);
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    let mut idx = 0;

    for chunk in chunks {
        unsafe {
            let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

            let cmp_eq = _mm256_cmpeq_epi64(v, value_vec);
            let cmp_ne = _mm256_xor_si256(cmp_eq, _mm256_set1_epi64x(-1));
            let mask = _mm256_movemask_epi8(cmp_ne);

            if (mask & 0x000000FF) == 0xFF {
                indices.push(idx);
            }
            if (mask & 0x0000FF00) == 0xFF00 {
                indices.push(idx + 1);
            }
            if (mask & 0x00FF0000) == 0xFF0000 {
                indices.push(idx + 2);
            }
            if (mask as u32 & 0xFF000000) == 0xFF000000 {
                indices.push(idx + 3);
            }
        }

        idx += 4;
    }

    for (i, &v) in remainder.iter().enumerate() {
        if v != value {
            indices.push(idx + i);
        }
    }

    indices
}

#[cfg(target_arch = "x86_64")]
pub fn filter_ne_i64(data: &[i64], value: i64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }

    if is_x86_feature_detected!("avx2") {
        unsafe { filter_ne_i64_avx2(data, value) }
    } else {
        super::scalar::filter_ne_i64(data, value)
    }
}

#[cfg(target_arch = "x86_64")]
pub fn avg_i64(data: &[i64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }

    let sum = sum_i64(data);
    Some(sum as f64 / data.len() as f64)
}

#[cfg(target_arch = "x86_64")]
pub fn avg_f64(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }

    let sum = sum_f64(data);
    Some(sum / data.len() as f64)
}

#[cfg(target_arch = "x86_64")]
pub fn avg_i64_masked(data: &[i64], nulls: &[u8]) -> Option<f64> {
    assert_eq!(data.len(), nulls.len());

    if data.is_empty() {
        return None;
    }

    let sum = sum_i64_masked(data, nulls);
    let count = count_nonzero_u8(nulls);

    if count == 0 {
        None
    } else {
        Some(sum as f64 / count as f64)
    }
}

#[cfg(target_arch = "x86_64")]
pub fn avg_f64_masked(data: &[f64], nulls: &[u8]) -> Option<f64> {
    assert_eq!(data.len(), nulls.len());

    if data.is_empty() {
        return None;
    }

    let sum = sum_f64_masked(data, nulls);
    let count = count_nonzero_u8(nulls);

    if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    }
}
