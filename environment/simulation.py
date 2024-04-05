import simpy
import random
import numpy as np
import pandas as pd

from collections import OrderedDict


class Plate:
    def __init__(self, name, id, from_location, to_location, weight):
        self.name = name
        self.id = id
        self.from_location = from_location
        self.to_location = to_location
        self.weight = weight


class InputPoint:
    def __init__(self, env, name, id, location, plates, monitor):
        self.env = env
        self.name = name
        self.id = id
        self.location = location
        self.plates = plates
        self.monitor = monitor

        self.call = None
        self.action = env.process(self.run())

    def run(self):
        while len(self.plates) != 0:
            self.monitor.queue_storage[self.id] = self
            self.call = self.env.event()
            yield self.call

    def get_plate(self):
        plate = self.plates.pop()
        return plate


class Pile:
    def __init__(self, env, name, id, type, location, plates, monitor):
        self.env = env
        self.name = name
        self.id = id
        self.type = type
        self.location = location
        self.plates = plates
        self.monitor = monitor

        self.plates_stacked = []
        self.call = None

        if len(plates) > 0:
            self.action = env.process(self.run())

    def run(self):
        while len(self.plates) != 0:
            if type == "storage":
                self.monitor.queue_reshuffle[self.id] = self
            elif type == "retrieval":
                self.monitor.queue_retireval[self.id] = self
            else:
                print("Invalid type")

            self.call = self.env.event()
            yield self.call

    def get_plate(self):
        plate = self.plates.pop()
        return plate

    def put_plate(self, plate):
        self.plates_stacked.append(plate)


class OutputPoint:
    def __init__(self, env, name, id, type, location, IAT, monitor):
        self.env = env
        self.name = name
        self.id = id
        self.type = type
        self.location = location
        self.IAT = IAT
        self.monitor = monitor

        self.plates_retrieved = []
        self.call = None

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


class Crane:
    def __init__(self, env, name, id, x_velocity, y_velocity, safety_margin, weight_limit, initial_location,
                 other_crane, input_points, piles, output_points, monitor, row_range=(0, 1), bay_range=(0, 43)):
        self.env = env
        self.name = name
        self.id = id
        self.x_velocity = x_velocity
        self.y_velocity = y_velocity
        self.safety_margin = safety_margin
        self.weight_limit = weight_limit
        self.initial_location = initial_location
        self.other_crane = other_crane
        self.input_points = input_points
        self.piles = piles
        self.output_points = output_points
        self.monitor = monitor
        self.row_range = row_range
        self.bay_range = bay_range

        self.target_location = None
        self.target_location_coord = (-1.0, -1.0)
        self.current_location_coord = initial_location.coord
        self.safety_xcoord = -1.0

        self.offset_location = None
        self.loading_location_ids = []
        self.unloading_location_ids = []
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

        self.coord_to_id = {}
        for id, input_point in input_points.items():
            for i in range(row_range[0], row_range[1] + 1):
                coord = (input_point[0], i)
                self.coord_to_id[coord] = id
        for id, pile in piles.items():
            self.coord_to_id[pile.coord] = id
        for id, output_point in output_points.items():
            for i in range(row_range[0], row_range[1] + 1):
                coord = (output_point[0], i)
                self.coord_to_id[coord] = id

        self.move_process = None
        self.action = env.process(self.run())

    def run(self):
        while True:
            self.monitor.queue_sequencing[self.id] = self
            self.event_sequencing = self.env.event()
            target_location_id, target_location_code = yield self.event_sequencing

            if target_location_code == "None":
                self.idle = self.env.event()

                waiting_start = self.env.now
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, "Waiting Start", crane=self.name,
                                        location=self.current_location.name, plate=None)

                yield self.idle

                waiting_finish = self.env.now
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, "Waiting Finish", crane=self.name,
                                        location=self.current_location.name, plate=None)

                self.idle_time += waiting_finish - waiting_start
            else:
                self.status = "loading"

                if target_location_code == "input_point":
                    self.job_type = "storage"
                    self.offset_location = self.input_points[target_location_id]
                elif target_location_code == "pile":
                    self.job_type = "reshuffle"
                    self.offset_location = self.piles[target_location_id]
                elif target_location_code == "output_point":
                    self.job_type = "retrieval"
                    self.offset_location = self.output_points[target_location_id]

                self.monitor.queue_loading[self.id] = self
                self.event_loading = self.env.event()
                self.loading_location_ids = yield self.event_loading
                self.unloading_location_ids = self.loading_location_ids[::-1]

                self.move_process = self.env.process(self.move())
                yield self.move_process
                self.loading_location_ids = []

                self.status = "unloading"

                self.move_process = self.env.process(self.move())
                yield self.move_process
                self.unloading_location_ids = []

            self.status = "idle"

    def move(self):
        if self.status == "loading":
            location_ids = self.loading_location_ids[:]
        elif self.status == "unloading":
            location_ids = self.unloading_location_ids[:]
        else:
            print("Invalid Access")
            return

        idx = 0
        while True:
            self.start_time = self.env.now
            target_location_id = location_ids[idx]
            if self.job_type == "storage" or self.job_type == "reshuffle":
                self.target_location = self.piles[target_location_id]
                self.target_location_coord = self.target_location.coord
            elif self.job_type == "retrieval":
                self.target_location = self.output_points[target_location_id]
                self.target_location_coord = (self.target_location.coord[0], self.current_location_coord[1])

            flag, safety_xcoord = self.check_interference()
            if flag:
                self.other_crane.move_process.interrupt()

                self.monitor.queue_prioritizing[self.id] = self
                self.event_prioritizing = self.env.event()
                priority = yield self.event_prioritizing
            else:
                priority = "high"

            if priority == "high":
                dx = self.target_location_coord[0] - self.current_location_coord[0]
                dy = self.target_location_coord[1] - self.current_location_coord[1]
                avoidance = False
            else:
                dx = safety_xcoord - self.current_location_coord[0]
                dy = self.target_location_coord[1] - self.current_location_coord[1]
                avoidance = True

            try:
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, "Move_from", crane=self.name,
                                        location=self.current_location.name, plate=None)

                predicted_moving_time = min(abs(dx) / self.x_velocity, abs(dy) / self.y_velocity)
                yield self.env.timeout(predicted_moving_time)
            except simpy.Interrupt as i:
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, "Move_to", crane=self.name,
                                        location=self.current_location.name, plate=None)
            else:
                if not avoidance:
                    idx += 1
                    if self.status == "loading":
                        plate = self.target_location.get_plate()
                        self.plates.append(plate)
                        if self.monitor.record_events:
                            self.monitor.record(self.env.now, "Pick_up", crane=self.name,
                                                location=self.target_location.name, plate=plate.name)
                    else:
                        plate = self.plates.pop()
                        self.target_location.put_plate(plate)
                        if self.monitor.record_events:
                            self.monitor.record(self.env.now, "Put_down", crane=self.name,
                                                location=self.target_location.name, plate=plate.name)
            finally:
                actual_moving_time = self.env.now - self.start_time
                x_coord = self.current_location_coord[0] + actual_moving_time * self.x_velocity * np.sign(dx)
                y_coord = self.current_location_coord[1] + actual_moving_time * self.y_velocity * np.sign(dy)
                x_coord = np.clip(x_coord, self.bay_range[0], self.bay_range[1])
                y_coord = np.clip(y_coord, self.row_range[0], self.row_range[1])
                self.current_location_coord = (x_coord, y_coord)

    def check_interference(self):
        if self.other_crane.status == "idle":
            flag = False
            safety_xcoord = None
        else:
            dx = self.target_location_coord[0] - self.current_location_coord[0]
            dy = self.target_location_coord[1] - self.current_location_coord[1]
            direction = np.sign(dx)
            moving_time = max(abs(dx) / self.x_velocity, abs(dy) / self.y_velocity)

            elapsed_time = self.env.now - self.other_crane.start_time
            dx_other = self.other_crane.target_location_coord[0] - self.other_crane.current_location_coord[0]
            dy_other = self.other_crane.target_location_coord[1] - self.other_crane.current_location_coord[1]
            direction_other = np.sign(dx_other)
            moving_time_other = max(abs(dx_other) / self.other_crane.x_velocity,
                                    abs(dy_other) / self.other_crane.y_velocity)
            moving_time_other = moving_time_other - elapsed_time

            min_moving_time = min(moving_time, moving_time_other)
            xcoord = self.current_location_coord[0] + min_moving_time * self.x_velocity * direction
            xcoord_other = (self.other_crane.current_location_coord[0]
                            + (min_moving_time + elapsed_time) * self.other_crane.x_velocity * direction_other)

            if self.id == 0 and xcoord >= xcoord_other - self.safety_margin:
                flag = True
                safety_xcoord = self.other_crane.target_location_coord[0] - self.safety_margin - 1
            elif self.id == 1 and xcoord <= xcoord_other + self.safety_margin:
                flag = True
                safety_xcoord = self.other_crane.target_location_coord[0] + self.safety_margin + 1
            else:
                flag = False
                safety_xcoord = None

        return flag, safety_xcoord