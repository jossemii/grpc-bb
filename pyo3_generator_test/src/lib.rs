use futures::pin_mut;
use futures::stream::{ Stream, StreamExt}; // Importa Stream y StreamExt
use futures::executor;
use async_stream::stream;
use pyo3::prelude::*;

fn zero_to_three() -> impl Stream<Item = u32> {
    stream! {
        for i in 0..16 {
            yield i;
        }
    }
}

async fn sumatory() -> u32 {
    let mut i: u32 = 0;
    let s = zero_to_three();
    pin_mut!(s);
    while let Some(item) = s.next().await {
        i += item;
    }
    i
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    let i = executor::block_on(sumatory());
    return Ok(i.to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyo3_simple_generator(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
