use log::trace;

const EPSILON: f64 = 1e-6;

pub fn compute_kelly_bet(returns: &[f64]) -> f64 {
    debug_assert!(!returns.is_empty());
    let pr = 1.0 / returns.len() as f64;

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
        let d = returns
            .iter()
            .map(|&r| (r * pr) / (1.0 + f * r))
            .sum::<f64>();

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

pub fn expected_log_portfolio_return(
    fractions: &[f64],
    returns: &[f64],
    probabilities: &[f64],
) -> f64 {
    assert_eq!(returns.len(), fractions.len() * probabilities.len());

    returns
        .chunks_exact(fractions.len())
        .zip(probabilities)
        .map(|(r, &p)| {
            p * f64::ln(1.0 + fractions.iter().zip(r).map(|(&f, &r)| f * r).sum::<f64>())
        })
        .sum::<f64>()
}

pub fn optimize_portfolio(
    num_positions: usize,
    returns: &[f64],
    probabilities: &[f64],
) -> Vec<f64> {
    if num_positions == 0 {
        return Vec::new();
    }

    assert_eq!(returns.len(), num_positions * probabilities.len());

    // TODO: re-tune this
    const MAX_ITERS: i32 = 1 << 20;

    let mut step = f64::sqrt(num_positions as f64);
    let mut fractions = vec![0.0; num_positions];
    let mut prev_exp_return = f64::MIN;
    let mut grad = vec![0.0; num_positions];

    for i in 0..MAX_ITERS {
        // Compute the gradient and expected return
        let exp_return = returns
            .chunks_exact(num_positions)
            .zip(probabilities)
            .map(|(r, &p)| {
                let denom = 1.0 + fractions.iter().zip(r).map(|(&f, &r)| f * r).sum::<f64>();
                grad.iter_mut()
                    .zip(r)
                    .for_each(|(g, &r)| *g += (r * p) / denom);
                p * f64::ln(denom)
            })
            .sum::<f64>();

        if !exp_return.is_finite() {
            step /= 2.0;
            fractions.fill(0.0);
            grad.fill(0.0);
            prev_exp_return = f64::MIN;
            continue;
        }

        if exp_return <= prev_exp_return {
            step /= 2.0;
        } else if exp_return - prev_exp_return < EPSILON {
            trace!("Iterations: {i}");
            break;
        }

        prev_exp_return = exp_return;

        // Compute the norm of the gradient and make sure it's constraint-compliant
        let norm = fractions
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

        let mul = step / norm;

        // Apply gradient to return vector and reset it to zero
        fractions.iter_mut().zip(&mut grad).for_each(|(f, g)| {
            *f += *g * mul;
            *g = 0.0;
        });

        if i == MAX_ITERS - 1 {
            trace!("Iterations maxed out");
        }
    }

    fractions
}
