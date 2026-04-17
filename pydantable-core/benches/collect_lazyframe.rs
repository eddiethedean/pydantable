use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pyo3::prelude::*;

#[cfg(feature = "polars_engine")]
use polars::prelude::*;

#[cfg(all(feature = "polars_engine", feature = "bench"))]
use pydantable_core::bench_collect_lazyframe;

fn bench_collect(c: &mut Criterion) {
    #[cfg(all(feature = "polars_engine", feature = "bench"))]
    {
        pyo3::prepare_freethreaded_python();
        let mut group = c.benchmark_group("collect_lazyframe");
        for n in [100_000usize, 1_000_000] {
            let vals: Vec<i64> = (0..n as i64).collect();
            let df = df!("x" => &vals).unwrap();

            for streaming in [false, true] {
                group.bench_with_input(
                    BenchmarkId::new(if streaming { "streaming" } else { "in_memory" }, n),
                    &n,
                    |b, _| {
                        b.iter(|| {
                            Python::with_gil(|py| {
                                let lf = df.clone().lazy().select([col("x").sum().alias("s")]);
                                let _ =
                                    bench_collect_lazyframe(py, black_box(lf), streaming).unwrap();
                            })
                        })
                    },
                );
            }
        }
        group.finish();
    }
}

criterion_group!(benches, bench_collect);
criterion_main!(benches);
