use log::trace;

const EPSILON: f64 = 1e-5;

pub fn compute_kelly_bet(returns: &[f64], probabilities: &[f64]) -> f64 {
    debug_assert_eq!(returns.len(), probabilities.len());
    debug_assert!(!returns.is_empty());

    let mut min = f64::MAX;
    let mut max = f64::MIN;
    for &r in returns {
        min = f64::min(r, min);
        max = f64::max(r, max);
    }

    if min >= 0.0 {
        return f64::INFINITY;
    }

    if max <= 0.0 {
        return f64::NEG_INFINITY;
    }

    let mut f_min = -1.0 / max;
    let mut f_max = -1.0 / min;

    loop {
        let f = (f_min + f_max) / 2.0;
        let d = derivative_at(f, returns, probabilities);

        if d.abs() < EPSILON {
            return f;
        }

        // Optimal bet is higher
        if d > 0.0 {
            f_min = f;
        }
        // Optimal bet is lower
        else {
            f_max = f;
        }
    }
}

fn derivative_at(f: f64, returns: &[f64], probabilities: &[f64]) -> f64 {
    debug_assert_eq!(returns.len(), probabilities.len());
    returns
        .iter()
        .zip(probabilities)
        .map(|(&r, &pr)| (r * pr) / (1.0 + f * r))
        .sum()
}

pub fn expected_log_portfolio_return<T>(
    fractions: &[f64],
    returns: &[T],
    probabilities: &[f64],
) -> f64
where
    T: AsRef<[f64]>,
{
    returns
        .iter()
        .zip(probabilities)
        .map(|(r, &p)| {
            p * f64::ln(
                1.0 + fractions
                    .iter()
                    .zip(r.as_ref())
                    .map(|(&f, &r)| f * r)
                    .sum::<f64>(),
            )
        })
        .sum::<f64>()
}

pub fn balance_portfolio<T>(positions: usize, returns: &[T], probabilities: &[f64]) -> Vec<f64>
where
    T: AsRef<[f64]>,
{
    const STEP: f64 = 0.05;

    let mut ret = vec![0.0; positions];
    let mut grad = vec![0.0; positions];
    let mut i = 0;

    loop {
        // Compute the gradient
        returns.iter().zip(probabilities).for_each(|(r, &p)| {
            let r = r.as_ref();
            let denom = 1.0 + ret.iter().zip(r).map(|(&f, &r)| f * r).sum::<f64>();
            grad.iter_mut()
                .zip(r)
                .for_each(|(g, &r)| *g += (r * p) / denom);
        });

        // Compute the norm of the gradient and make sure it's constraint-compliant
        let norm = ret
            .iter_mut()
            .zip(&mut grad)
            .map(|(f, g)| {
                let g_val = *g;
                if *f + g_val < 0.0 {
                    *f = 0.0;
                    *g = 0.0;
                    0.0
                } else {
                    g_val * g_val
                }
            })
            .sum::<f64>()
            .sqrt();

        if norm < EPSILON {
            trace!("Iterations: {i}");
            return ret;
        }

        let mul = STEP / norm;

        // Apply gradient to return vector and reset it to zero
        ret.iter_mut().zip(&mut grad).for_each(|(f, g)| {
            *f += *g * mul;
            *g = 0.0;
        });

        i += 1;
        if i % 1024 == 0 {
            trace!("Iterations: {i}");
        }
    }
}
