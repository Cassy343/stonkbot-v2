use rust_decimal::{Decimal, MathematicalOps};

mod sealed {
    pub trait AsReturn {
        fn as_return(&self) -> Self;
    }

    pub trait WeightUpdate<U> {
        fn weight_update(&self, eta: U) -> Self;
    }
}

pub(crate) use sealed::*;

pub fn mwu_multiplier<T, U>(delta: Delta<T>, eta: U) -> T
where
    T: AsReturn + WeightUpdate<U>,
{
    delta.into_return().weight_update(eta)
}

pub enum Delta<T> {
    Return(T),
    ChangePercent(T),
}

impl<T: AsReturn> Delta<T> {
    #[inline]
    fn into_return(self) -> T {
        match self {
            Self::Return(r) => r,
            Self::ChangePercent(cp) => cp.as_return(),
        }
    }
}

impl AsReturn for f64 {
    #[inline]
    fn as_return(&self) -> Self {
        1.0 + (self / 100.0)
    }
}

impl WeightUpdate<Decimal> for f64 {
    #[inline]
    fn weight_update(&self, eta: Decimal) -> Self {
        if !self.is_finite() || *self <= 0.0 {
            return 0.5;
        }

        let clamped = self.min(1.0 / 0.95).max(0.95);
        f64::powf(
            clamped,
            f64::try_from(eta).expect("Failed to convert eta to f64"),
        )
    }
}

impl AsReturn for Decimal {
    #[inline]
    fn as_return(&self) -> Self {
        Decimal::ONE + (self / Decimal::ONE_HUNDRED)
    }
}

#[inline]
fn clamp_return(r: Decimal) -> Decimal {
    let lower_bound = Decimal::new(95, 2);
    r.min(Decimal::ONE / lower_bound).max(lower_bound)
}

impl WeightUpdate<Decimal> for Decimal {
    #[inline]
    fn weight_update(&self, eta: Decimal) -> Self {
        clamp_return(*self).powd(eta)
    }
}

impl WeightUpdate<f64> for Decimal {
    #[inline]
    fn weight_update(&self, eta: f64) -> Self {
        clamp_return(*self).powf(eta)
    }
}
