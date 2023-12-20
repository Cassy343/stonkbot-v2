use libm::erf;
use log::trace;
use serde::Serialize;
use std::f64::consts::TAU;

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

        if norm < EPSILON {
            trace!("Iterations: {i}");
            break;
        }

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

fn laplace_cdf(x: f64, mean: f64, b: f64) -> f64 {
    0.5 + 0.5 * f64::signum(x - mean) * (1.0 - f64::exp(-f64::abs(x - mean) / b))
}

fn laplace_first_moment_a(x: f64, mean: f64, b: f64) -> f64 {
    -0.5 * (b - x) * f64::exp((x - mean) / b)
}

fn laplace_first_moment_b(x: f64, mean: f64, b: f64) -> f64 {
    -0.5 * (b + x) * f64::exp((mean - x) / b)
}

fn normal_cdf(x: f64, mean: f64, var: f64) -> f64 {
    0.5 + 0.5 * erf((x - mean) / f64::sqrt(2.0 * var))
}

fn normal_first_moment(x: f64, mean: f64, var: f64) -> f64 {
    let x_off = x - mean;
    let half_mean = 0.5 * mean;
    let two_var = 2.0 * var;

    half_mean + half_mean * erf(x_off / f64::sqrt(two_var))
        - f64::sqrt(var / TAU) * f64::exp((-1.0 / two_var) * x_off * x_off)
}

pub fn heuristic(
    params: NormalParameters,
    mean_offset: f64,
    hold_time: u32,
    max_hold_time: u32,
    baseline_return: f64,
) -> f64 {
    let time_delta = (max_hold_time - hold_time) as f64;
    let mean = time_delta * params.mean + mean_offset;
    let var = time_delta * params.var;
    let target_return = max_hold_time as f64 * baseline_return;

    let prob_meet_goal = 1.0 - normal_cdf(target_return, mean, var);
    let prob_pos_return = 1.0 - normal_cdf(0.0, mean, var);

    2.0 * prob_meet_goal + prob_pos_return
}

pub fn expected_log_portfolio_return_normal(
    fractions: &[f64],
    parameters: &[NormalParameters],
) -> f64 {
    assert_eq!(fractions.len(), parameters.len());

    let meta = parameters
        .iter()
        .map(|&params| NormalMeta::from(params))
        .collect::<Vec<_>>();

    let mut exp_log_return = 0.0;

    for selector in 0..1u32 << fractions.len() {
        let mut prob = 1.0;
        let mut ret = 1.0;

        for (i, meta) in meta.iter().enumerate() {
            let (p, r) = if selector & (1u32 << i) == 0 {
                (meta.loss_prob, meta.exp_loss)
            } else {
                (meta.gain_prob, meta.exp_gain)
            };

            prob *= p;
            ret += fractions[i] * r;
        }

        exp_log_return += prob * f64::ln(ret);
    }

    exp_log_return
}

pub fn optimize_portfolio_normal(parameters: &[NormalParameters]) -> Vec<f64> {
    if parameters.is_empty() {
        return Vec::new();
    }

    let meta = parameters
        .iter()
        .map(|&params| NormalMeta::from(params))
        .collect::<Vec<_>>();

    // TODO: re-tune this
    const MAX_ITERS: i32 = 1 << 20;

    let mut step = f64::sqrt(parameters.len() as f64);
    let mut fractions = vec![0.0; parameters.len()];
    let mut prev_exp_return = f64::MIN;
    let mut grad = vec![0.0; parameters.len()];

    for i in 0..MAX_ITERS {
        // Compute the gradient and expected return
        let mut exp_return = 0.0;

        for selector in 0..1u32 << fractions.len() {
            let mut prob = 1.0;
            let mut ret = 1.0;

            for (i, meta) in meta.iter().enumerate() {
                let (p, r) = if selector & (1u32 << i) == 0 {
                    (meta.loss_prob, meta.exp_loss)
                } else {
                    (meta.gain_prob, meta.exp_gain)
                };

                prob *= p;
                ret += fractions[i] * r;
            }

            exp_return += prob * f64::ln(ret);

            for (i, meta) in meta.iter().enumerate() {
                let r = if selector & (1u32 << i) == 0 {
                    meta.exp_loss
                } else {
                    meta.exp_gain
                };

                grad[i] += prob * r / ret;
            }
        }

        if !exp_return.is_finite() {
            step /= 2.0;
            fractions.fill(0.0);
            grad.fill(0.0);
            prev_exp_return = f64::MIN;
            continue;
        }

        if exp_return <= prev_exp_return {
            step /= 2.0;
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

        if norm < EPSILON {
            trace!("Iterations: {i}");
            break;
        }

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

#[derive(Clone, Copy, Serialize, Debug)]
pub struct NormalParameters {
    pub mean: f64,
    pub var: f64,
}

#[derive(Debug)]
struct NormalMeta {
    exp_loss: f64,
    loss_prob: f64,
    exp_gain: f64,
    gain_prob: f64,
}

impl From<NormalParameters> for NormalMeta {
    fn from(NormalParameters { mean, var }: NormalParameters) -> Self {
        let loss_prob = normal_cdf(0.0, mean, var);
        let gain_prob = 1.0 - loss_prob;
        let moment = normal_first_moment(0.0, mean, var);
        let exp_loss = f64::exp(moment / loss_prob) - 1.0;
        let exp_gain = f64::exp((mean - moment) / gain_prob) - 1.0;

        Self {
            exp_loss,
            loss_prob,
            exp_gain,
            gain_prob,
        }
    }
}
