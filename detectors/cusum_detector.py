import numpy as np
from detector import ChangeDetector

class CusumDetector(ChangeDetector):

    def __init__(self, delta=0.005, lambd=50):
        super( CusumDetector, self ).__init__()
        self.delta = delta
        self.lambd = lambd
        self.mean_ = 0
        self.sum_ = 0
        self.n = 0

    def update(self, new_value):
        super(CusumDetector, self).update(new_value)
        self.n += 1
        self.mean_ = self.mean_ + (new_value - self.mean_) / self.n
        self.sum_ = self.sum_ + new_value - self.mean_ - self.delta

    def check_change(self, new_value):
        self.is_change_detected = False
        if np.abs(np.sum(self.sum_)) > self.lambd:
            self.is_change_detected = True
            self.n = 0
            self.sum_ = 0
            self.mean_ = 0
