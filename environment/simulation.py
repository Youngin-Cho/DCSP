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


class Crane:
    def __init__(self, env, name, id, x_velocity, y_velocity, safety_margin, weight_limit, initial_location,
                 other_crane, piles, conveyors, monitor):
        self.env = env
        self.name = name
        self.id = id
        self.x_velocity = x_velocity
        self.y_velocity = y_velocity
        self.safety_margin = safety_margin
        self.weight_limit = weight_limit
        self.current_location = initial_location
        self.other_crane = other_crane
        self.piles = piles
        self.conveyors = conveyors
        self.monitor = monitor

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

        self.idle_time = 0.0
        self.empty_travel_time = 0.0
        self.avoiding_time = 0.0

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
                loading_location_ids = yield self.event_loading

                for location_id in loading_location_ids:
                    self.loading_locations.append(location_id)

                yield self.env.process(self.move())

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
            self.target_location = location_list[idx]
            flag = self.check_interference()
            if flag:
                self.monitor.queue_prioritizing[self.id] = self
                self.event_prioritizing = self.env.event()
                priority = yield self.event_prioritizing
            else:
                priority = "high"

            try:
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, location=self.name, ship=ship.name,
                                        operation=operation.name, event="Working Started", info=priority_score)
                yield self.env.timeout(duration)
            except simpy.Interrupt as i:
                if self.monitor.record_events:
                    self.monitor.record(self.env.now, location=self.name, ship=ship.name,
                                        operation=operation.name, event="Working Interrupted", info=priority_score)
                operation.progress += (self.env.now - working_start)
                self.monitor.operations_interrupted[operation.id] = operation
                interrupted = True
            else:

    def check_interference(self):
        if self.other_crane.status == "idle":
            flag = False
        else:
            dx = self.target_location[0] - self.current_location[0]
            dy = self.target_location[1] - self.current_location[1]
            direction_crane = np.sign(dx)
            moving_time_crane = max(abs(dx) / self.x_velocity, abs(dy) / self.y_velocity)

            trajectory_opposite_crane = []
            if self.other_crane.status == "loading":
                target_location = self.other_crane.from_locations[0]
            elif self.other_crane.status == "unloading":
                location_list = self.other_crane.to_locations[0]
            else:
                location_list = []

            current_coord_opposite_crane = crane.opposite.current_coord
            for location in location_list:
                if location in self.piles.keys():
                    dx = self.piles[location].coord[0] - current_coord_opposite_crane[0]
                    dy = self.piles[location].coord[1] - current_coord_opposite_crane[1]
                else:
                    dx = self.conveyors[location].coord[0] - current_coord_opposite_crane[0]
                    dy = 0

                direction_opposite_crane = np.sign(dx)
                moving_time_opposite_crane = max(abs(dx) / crane.opposite.x_velocity,
                                                 abs(dy) / crane.opposite.y_velocity)
                if location in self.piles.keys():
                    coord_opposite_crane = self.piles[location].coord
                else:
                    coord_opposite_crane = (self.conveyors[location].coord[0], current_coord_opposite_crane[1])
                trajectory_opposite_crane.append(
                    (direction_opposite_crane, moving_time_opposite_crane, coord_opposite_crane))
                current_coord_opposite_crane = coord_opposite_crane

            current_coord_opposite_crane = crane.opposite.current_coord
            moving_time_cum = 0.0
            avoidance = False
            safety_xcoord = None
            for i, move in enumerate(trajectory_opposite_crane):
                if moving_time_crane <= move[1] + moving_time_cum:
                    min_moving_time = moving_time_crane
                    min_crane = crane.name
                else:
                    min_moving_time = move[1] + moving_time_cum
                    min_crane = crane.opposite.name

                xcoord_crane = crane.current_coord[0] + min_moving_time * crane.x_velocity * direction_crane
                xcoord_opposite_crane = (current_coord_opposite_crane[0] + (min_moving_time - moving_time_cum)
                                         * crane.opposite.x_velocity * move[0])

                if (crane.name == 'Crane-1' and xcoord_crane > xcoord_opposite_crane - self.safety_margin) \
                        or (
                        crane.name == 'Crane-2' and xcoord_crane < xcoord_opposite_crane + self.safety_margin):
                    avoidance = True
                    if crane.name == 'Crane-1':
                        safety_xcoord = min(
                            [temp[2][0] for temp in trajectory_opposite_crane[i:]]) - self.safety_margin
                    else:
                        safety_xcoord = max(
                            [temp[2][0] for temp in trajectory_opposite_crane[i:]]) + self.safety_margin
                    break
                else:
                    if crane.name == min_crane:
                        check_same_pile = ((crane.loading and crane.opposite.loading)
                                           or (crane.loading and crane.opposite.unloading)
                                           or (crane.unloading and crane.opposite.unloading))
                        same_pile = False
                        if check_same_pile:
                            for temp in trajectory_opposite_crane[i:]:
                                if crane.target_coord[0] == temp[2][0] and crane.target_coord[1] == temp[2][1]:
                                    same_pile = True

                        if same_pile:
                            avoidance = True
                            if crane.name == "Crane-1":
                                safety_xcoord = crane.target_coord[0] - self.safety_margin
                            else:
                                safety_xcoord = crane.target_coord[0] + self.safety_margin
                        else:
                            avoidance = False
                            safety_xcoord = None
                        break
                    else:
                        if len(trajectory_opposite_crane) == i + 1:
                            avoidance = False
                            safety_xcoord = None
                        else:
                            current_coord_opposite_crane = move[2]
                            moving_time_cum = min_moving_time

        return avoidance, safety_xcoord














class Conveyor:
    def __init__(self, name, coord, IAT):
        self.name = name
        self.coord = [coord]
        self.IAT = IAT

        self.plates_retrieved = []


class Management:
    def __init__(self, env, df_storage, df_reshuffle, df_retrieval, cranes, piles, conveyors,
                 row_range=(0, 1), bay_range=(0, 43), input_points=(0,), output_points=(22, 26, 43)):
        self.env = env
        self.df_storage = df_storage
        self.df_reshuffle = df_reshuffle
        self.df_retrieval = df_retrieval
        self.cranes = cranes
        self.piles = piles
        self.conveyors = conveyors
        self.row_range = row_range
        self.bay_range = bay_range
        self.input_points = input_points
        self.output_points = output_points

        self.retrieval_events = simpy.Store(env)

        self.sequencing_required = False
        self.prioritizing_required = False
        self.loading_required = False


    def run(self):
        yield get


    def initialize(self):





