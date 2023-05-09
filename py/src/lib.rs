use pyo3::prelude::*;

mod options;
pub use options::*;

#[pymodule]
fn showbiz(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  m.add_class::<Options>()?;
  Ok(())
}
