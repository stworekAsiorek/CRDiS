import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

class ChangeDetector(object):
    def __init__(self):
        self.is_change_detected = False
        self.sequence_size = 0
        self.sequence = []
        self.current_value = 0
        self.previous_value = 0

    def update(self, new_value):
        self.sequence.append(new_value)
        self.sequence_size += 1

    def check_change(self, new_value):
        pass

    def get_parameters(self):
        parameters_dict = {}
        for k, v in self.__dict__.items():
            if k.endswith('_'):
                parameters_dict[k] = v

        return parameters_dict

    def step(self, new_value):
        self.update(new_value)
        self.check_change(new_value)

        return self.get_parameters()

class OnlineSimulator(object):
    def __init__(self, change_detectors, sequences, seqs_names):
        self.sequences = sequences
        self.sequences_names = seqs_names
        self.change_detectors = change_detectors
        self.sequence_size = len(sequences[0])
        self.detected_change_points = [[] for i in range(len(self.sequences))]
        self.rules = [[] for i in range(len(self.sequences))]
        self.parameters_history = [defaultdict(list) for i in range(len(self.sequences))]

    def get_detected_changes(self):
        return self.detected_change_points

    def get_rules(self):
        return self.rules

    def run(self, plot=True, **kwargs):
        parameters_history = [defaultdict(list) for i in range(len(self.sequences))]

        for i in range(0, self.sequence_size):
            for j, seq in enumerate(self.sequences):
                detector = self.change_detectors[j]

                value = seq[i]
                res = detector.step(value)

                for k, v in res.items():
                    #print(k, v)
                    parameters_history[j][k].append(v)

                if detector.is_change_detected is True:
                    prev_at = self.detected_change_points[j][-1].at_ if len(self.detected_change_points[j]) > 0 else 0
                    prev_value_len = i - prev_at

                    change_point = ChangePoint(detector.previous_value, detector.current_value, i, prev_value_len, self.sequences_names[j])
                    self.detected_change_points[j].append(change_point)
                    print(change_point)

                if i == self.sequence_size - 1:
                    change_point = ChangePoint(detector.current_value, -1, i, 0, self.sequences_names[j])
                    self.detected_change_points[j].append(change_point)
                    print(change_point)

                # if i == 0:
                #     change_point = ChangePoint(-1, detector.current_value, i, self.sequences_names[j])
                #     self.detected_change_points[j].append(change_point)
                #     print(self.sequences_names[j], "changed from:", change_point.from_, "to:", change_point.to_, "at: ",
                #           change_point.at_)


                #last seq - target
                if j == len(self.sequences) - 1:
                    if detector.is_change_detected is True:
                        for m, change_point_list in enumerate(self.detected_change_points):
                            for n, change_point in enumerate(change_point_list):
                                last_detected_target =  self.detected_change_points[j][-2].at_ if len(self.detected_change_points[j]) > 1 else 0
                                if change_point.at_ >= last_detected_target:
                                    rule = Rule(change_point.prev_value_len_, change_point.from_, self.sequences_names[m])


                                    self.rules[m].append(rule)
                                    print(rule)

                                    #last change
                                    if n == len(change_point_list) - 1:
                                        lastRule = Rule(i - change_point_list[n-1].at_, change_point_list[n-1].to_, self.sequences_names[m])
                                        self.rules[m].append(lastRule)
                                        print(lastRule)



        def dict_to_arrays(ddict):
            new_dict = {}
            for k, v in ddict.items():
                new_dict[k] = np.array(v)
            return new_dict

        for i in range(0, len(self.sequences)):
            parameters_history[i] = dict_to_arrays(parameters_history[i])
            self.parameters_history[i] = parameters_history[i]

        if plot is True:
            self.display_results(**kwargs)

        return detector.is_change_detected

    def display_results(self, sequence_name='Sequence', **kwargs):
        for i in range(0, len(self.sequences)):
            sequence = self.sequences[i]
            detector = self.change_detectors[i]
            parameters_history = self.parameters_history[i]
            detected_change_points = self.detected_change_points[i]

            plotcount = 1 + len(parameters_history)
            fig, axes = plt.subplots(nrows=plotcount, ncols=1, sharex=True,
                                     figsize=(12, plotcount*3))

            # Plot the sequence
            if plotcount > 1:
                ax = axes[0]
            elif plotcount == 1:
                ax = axes

            ax.plot(sequence, 'b.')
            ax.plot(sequence, 'b-', alpha=0.25)

            ax.set_title(sequence_name)

            ax.set_ylim(
                np.nanmin(sequence)*.5,
                np.nanmax(sequence)*1.5)
            ax.set_xlim(0, len(sequence))
            xl = ax.get_xticks()
            ticks = xl #- int(2/3 * len(sequence))

            ax.set_xticklabels(ticks)

            # Plot a horizontal line where the change_point is detected
            for change_point in detected_change_points:
                ax.vlines(x=change_point.at_, ymin=0, ymax=ax.get_ylim()[1],
                      colors='r', linestyles='dotted')

            # Plot each parameter
            for ii, (res_name, res_values) in enumerate(parameters_history.items()):
                ax = axes[ii+1]
                ax.plot(res_values, 'g.', alpha=0.7)
                ax.set_title("Parameter #{}: {}".format(ii+1, res_name))

                for change_poin in detected_change_points:
                    ax.vlines(x=change_poin.at_, ymin=0, ymax=ax.get_ylim()[1],
                          colors='r', linestyles='dotted')

        plt.show()

class ChangePoint(object):
    def __init__(self, from_, to_, at_, prev_value_len_, attr_name_):
        self.from_ = from_
        self.to_ = to_
        self.at_ = at_
        self.prev_value_len_ = prev_value_len_
        self.attr_name = attr_name_

    def __repr__(self):
        return("(" + self.attr_name + " changed from:" + str(self.from_) + "(len= "+ str(self.prev_value_len_) + ") to:" + str(self.to_) + " at: " + str(self.at_) + ")")

class Rule(object):
    def __init__(self, len_, value_, attr_name_):
        self.len_ = len_
        self.value_ = value_
        self.attr_name_ = attr_name_

    def __repr__(self):
        return(str(self.attr_name_) + ": " + str(self.len_) + " times " + str(self.value_))