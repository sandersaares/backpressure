use std::collections::VecDeque;

/// A gauge that keeps track of the peak value over a window of the last N instances.
pub struct Peaker<const N: usize> {
    value: usize,
    history: VecDeque<usize>,
    peak: usize,
    peak_count: usize,
}

impl<const N: usize> Peaker<N> {
    pub const fn new() -> Self {
        Self {
            value: 0,
            history: VecDeque::new(),
            peak: 0,
            peak_count: 0,
        }
    }

    pub fn get(&self) -> usize {
        self.value
    }

    pub fn peak(&self) -> usize {
        self.peak
    }

    pub fn set(&mut self, value: usize) -> usize {
        self.value = value;

        #[expect(clippy::comparison_chain)] // Ssshhhh, this code is perfectly fine.
        if value > self.peak {
            self.peak = value;
            self.peak_count = 0;
        } else if value == self.peak {
            self.peak_count += 1;
        }

        if self.history.len() == N {
            let popped = self.history.pop_front();

            if popped.unwrap() == self.peak {
                self.peak_count -= 1;

                if self.peak_count == 0 {
                    self.peak = *self.history.iter().max().unwrap_or(&0);
                    self.peak_count = self.history.iter().filter(|&&x| x == self.peak).count();
                }
            }
        }

        self.history.push_back(value);
        value
    }
}
