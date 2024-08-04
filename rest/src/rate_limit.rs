use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use tokio::{sync::Mutex, time::sleep};

const ONE_MINUTE: Duration = Duration::from_millis(60 * 1000);

pub struct RateLimiter {
    // Invariant: the instants in the queue are sorted from oldest to newest
    request_queue: Mutex<VecDeque<Instant>>,
    rate_limit: usize,
    unthrottled_budget: usize,
    throttling_duration: Duration,
}

impl RateLimiter {
    pub fn new(rate_limit: usize, min_rate: usize) -> Self {
        // We can do a lossy cast here since this value will never be greater than 60,001
        let throttling_duration = Duration::from_millis((60_000 / min_rate + 1) as u64);

        Self {
            request_queue: Mutex::new(VecDeque::with_capacity(rate_limit)),
            rate_limit,
            unthrottled_budget: rate_limit - min_rate,
            throttling_duration,
        }
    }

    pub async fn throttle_request(&self) {
        let mut guard = self.request_queue.lock().await;
        assert!(guard.len() <= self.rate_limit);

        // Step 1: remove requests outside the one minute rolling window
        loop {
            match guard.front().copied() {
                Some(instant) if instant.elapsed() >= ONE_MINUTE => {
                    guard.pop_front();
                }
                _ => break,
            }
        }

        // Step 2: sleep if needed
        match (guard.front(), guard.len()) {
            // Queue is full, we need to halt any new requests until we fall below the rate limit
            (Some(instant), len) if len == self.rate_limit => {
                // We need to wait for this request to be older than one minute
                let elapsed = instant.elapsed();
                if elapsed <= ONE_MINUTE {
                    sleep(ONE_MINUTE - elapsed).await;
                }
                guard.pop_front();
            }
            // The config guarantees that the rate limit is positive, so if the length is positive
            // there must be an item at the front of the queue, so this case is unreachable.
            (None, len) if len == self.rate_limit => unreachable!(),
            // We've exhausted the number of unthrottled requests in this rolling window. If we
            // hyper-pessimistically assume that all other requests in the queue were sent at the
            // same time the instant before this function call, then we'd need to spread out the
            // remaining requests in our budget over the next minute. The spacing required is
            // exactly `self.throttling_duration`
            (_, len) if len >= self.unthrottled_budget => sleep(self.throttling_duration).await,
            // We're not in danger of approaching the rate limit, so we don't need to throttle
            // requests.
            _ => (),
        }

        // Step 3: log the request
        guard.push_back(Instant::now());
    }
}
