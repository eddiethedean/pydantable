use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pyo3::prelude::*;

use pydantable_core::{BaseType, DTypeDesc};

#[cfg(feature = "polars_engine")]
use polars::prelude::*;

#[cfg(all(feature = "polars_engine", feature = "bench"))]
use pydantable_core::bench_series_to_py_list;

fn bench_series_to_py_list_int64(c: &mut Criterion) {
    #[cfg(all(feature = "polars_engine", feature = "bench"))]
    {
        pyo3::prepare_freethreaded_python();
        let dtype = DTypeDesc::scalar_nullable(BaseType::Int);
        let mut group = c.benchmark_group("series_to_py_list/int64");
        for n in [1_000usize, 100_000, 1_000_000] {
            let vals: Vec<Option<i64>> = (0..n as i64).map(Some).collect();
            let s = Series::new("x".into(), vals);
            group.bench_with_input(BenchmarkId::new("no_nulls", n), &n, |b, _| {
                b.iter(|| {
                    Python::with_gil(|py| {
                        let _ =
                            bench_series_to_py_list(py, black_box(&s), black_box(&dtype)).unwrap();
                    })
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_series_to_py_list_int64);
criterion_main!(benches);
