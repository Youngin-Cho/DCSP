import simpy
import random
import numpy as np
import pandas as pd

from collections import OrderedDict


class Plate:
    def __init__(self, name, id, from_pile, to_pile, weight):
        self.name = name
        self.id = id
        self.from_pile = from_pile
        self.to_pile = to_pile
        self.weight = weight


class Pile:
    def __init__(self, name, id, location, plates):
        self.name = name
        self.id = id
        self.location = location
        self.plates = plates

        self.blocking = []
        self.end_plates = []

    def get_plate(self):
        plate = self.plates.pop()
        return plate

    def put_plate(self, plate):
        self.plates.append(plate)


class Crane:
    def __init__(self, env, name, id, x_velocity, y_velocity, safety_margin, weight_limit, initial_location,
                 other_crane, input_points, piles, conveyors, monitor, row_range=(0, 1), bay_range=(0, 43)):
        self.env = env
        self.name = name
        self.id = id
        self.x_velocity = x_velocity
        self.y_velocity = y_velocity
        self.safety_margin = safety_margin
        self.weight_limit = weight_limit
        self.current_location = initial_location
        self.other_crane = other_crane
        self.input_points = input_points
        self.piles = piles
        self.conveyors = conveyors
        self.monitor = monitor
        self.row_range = row_range
        self.bay_range = bay_range

        self.target_location = (-1.0, -1.0)
        self.safety_xcoord = -1.0

        self.offset_location = None
        self.loading_locations = []
        self.unloading_locations = []
        self.plates = []

        self.idle = None
        self.job_type = None
        self.status = "idle"
        self.unloading = False

        self.event_sequencing = None
        self.event_loading = None
        self.event_prioritizing = None

        self.start_time = 0.0
        self.idle_time = 0.0
        self.empty_travel_time = 0.0
        self.avoiding_time = 0.0

        self.move_process = None

    def run(self):
        while True:
            self.monitor.queue_sequencing[self.id] = self
            self.event_sequencing = self.env.event()
            target_location_id, target_location_code = yield self.event_sequencing

            if target_location_code == "None":
                self.idle = self.env.event()

                waiting_start = self.env.now
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, "Waiting Start", crane=self.name, location=self.current_location)

                yield self.idle

                waiting_finish = self.env.now
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, "Waiting Finish", crane=self.name, location=self.current_location)

                self.idle_time += waiting_finish - waiting_start
            else:
                self.status = "loading"

                if target_location_code == "input_point":
                    self.job_type = "storage"
                    self.offset_location = self.piles[target_location_id]
                elif target_location_code == "pile":
                    self.job_type = "reshuffle"
                    self.offset_location = self.piles[target_location_id]
                elif target_location_code == "conveyor":
                    self.job_type = "retrieval"
                    self.offset_location = self.conveyors[target_location_id]

                self.monitor.queue_loading[self.id] = self
                self.event_loading = self.env.event()
                self.loading_locations = yield self.event_loading
                self.unloading_locations = self.loading_locations[::-1]

                self.move_process = self.env.process(self.move())
                yield self.move_process
                self.loading_locations = []

                self.status = "unloading"

                self.move_process = self.env.process(self.move())
                yield self.move_process
                self.unloading_locations = []

            self.status = "idle"

    def move(self):
        if self.status == "loading":
            location_list = self.loading_locations[:]
        elif self.status == "unloading":
            location_list = self.unloading_locations[:]
        else:
            print("Invalid Access")
            return

        idx = 0
        while True:
            self.start_time = self.env.now
            self.target_location = location_list[idx]
            flag, safety_xcoord = self.check_interference()
            if flag:
                self.other_crane.move_process.interrupt()

                self.monitor.queue_prioritizing[self.id] = self
                self.event_prioritizing = self.env.event()
                priority = yield self.event_prioritizing
            else:
                priority = "high"

            if priority == "high":
                dx = self.target_location[0] - self.current_location[0]
                dy = self.target_location[1] - self.current_location[1]
                avoidance = False
            else:
                dx = safety_xcoord - self.current_location[0]
                dy = self.target_location[1] - self.current_location[1]
                avoidance = True

            try:
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, "Move_from", crane=self.name,
                                        location=self.location_mapping[self.current_coord].name, plate=None)

                predicted_moving_time = min(abs(dx) / self.x_velocity, abs(dy) / self.y_velocity)
                yield self.env.timeout(predicted_moving_time)

            except simpy.Interrupt as i:
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, "Move_to", crane=self.name,
                                        location=self.location_mapping[self.current_coord].name, plate=None)
            else:
                if not avoidance:
                    idx += 1
                    if self.status == "loading":
                        plate_name = self.get_plate(self.target_location)
                        if self.monitor.record_events:
                            self.monitor.record(self.env.now, "Pick_up", crane=self.name,
                                                location=self.location_mapping[crane.current_coord].name,
                                                plate=plate_name)
                    else:
                        plate_name = self.put_plate(self.target_location)
                        if self.monitor.record_events:
                            self.monitor.record(self.env.now, "Put_down", crane=self.name,
                                                location=self.location_mapping[crane.current_coord].name,
                                                plate=plate_name)
            finally:
                actual_moving_time = self.env.now - self.start_time
                x_coord = self.current_location[0] + actual_moving_time * self.x_velocity * np.sign(dx)
                y_coord = self.current_location[1] + actual_moving_time * self.y_velocity * np.sign(dy)
                x_coord = np.clip(x_coord, self.bay_range[0], self.bay_range[1])
                y_coord = np.clip(y_coord, self.row_range[0], self.row_range[1])
                self.current_location = (x_coord, y_coord)


    def check_interference(self):
        if self.other_crane.status == "idle":
            flag = False
            safety_xcoord = None
        else:
            dx = self.target_location[0] - self.current_location[0]
            dy = self.target_location[1] - self.current_location[1]
            direction = np.sign(dx)
            moving_time = max(abs(dx) / self.x_velocity, abs(dy) / self.y_velocity)

            elapsed_time = self.env.now - self.other_crane.start_time
            dx_other = self.other_crane.target_location[0] - self.other_crane.current_location[0]
            dy_other = self.other_crane.target_location[1] - self.other_crane.current_location[1]
            direction_other = np.sign(dx_other)
            moving_time_other = max(abs(dx_other) / self.other_crane.x_velocity,
                                    abs(dy_other) / self.other_crane.y_velocity)
            moving_time_other = moving_time_other - elapsed_time

            min_moving_time = min(moving_time, moving_time_other)
            xcoord = self.current_location[0] + min_moving_time * self.x_velocity * direction
            xcoord_other = (self.other_crane.current_location[0]
                            + (min_moving_time + elapsed_time) * self.other_crane.x_velocity * direction_other)

            if self.id == 0 and xcoord >= xcoord_other - self.safety_margin:
                flag = True
                safety_xcoord = self.other_crane.target_location[0] - self.safety_margin - 1
            elif self.id == 1 and xcoord <= xcoord_other + self.safety_margin:
                flag = True
                safety_xcoord = self.other_crane.target_location[0] + self.safety_margin + 1
            else:
                flag = False
                safety_xcoord = None

        return flag, safety_xcoord

    def get_plate(self, location_id):
        if self.job_type == "storage":
            location = self.input_points[location_id]
        elif self.job_type == "reshuffle" or self.job_type == "retrieval":
            location = self.piles[location_id]
        else:
            print("invalid job type")

        plate = location.get_plate()
        self.plates.append(plate)
        return plate

    def put_plate(self, location_id, plate):
        if self.job_type == "storage" or self.job_type == "reshuffle":
            location = self.piles[location_id]
        elif self.job_type == "retrieval":
            location = self.conveyors[location_id]
        else:
            print("invalid job type")

        location.put_plate(plate)


class Conveyor:
    def __init__(self, env, name, id, coord, IAT, monitor):
        self.env = env
        self.name = name
        self.id = id
        self.coord = [coord]
        self.IAT = IAT
        self.monitor = monitor

        self.event_retrieval = None
        self.plates_retrieved = []

        self.action = env.process(self.run())

    def run(self):
        while True:
            IAT = np.random.geometric(self.IAT)
            yield self.env.timeout(IAT)
            if self.monitor.record_events:
                self.monitor.record(self.env.now, "Retrieval", crane=None, location=self.name, plate=None)

            self.monitor.queue_retireval[self.id] = self
            self.event_retrieval = self.env.event()
            yield self.event_retrieval

    def put_plate(self, plate):
        self.plates_retrieved.append(plate)



