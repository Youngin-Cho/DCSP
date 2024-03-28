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
    def __init__(self, env, name, id, velocity, safety_margin, weight_limit, initial_location,
                 other_crane, piles, conveyors, monitor):
        self.env = env
        self.name = name
        self.id = id
        self.velocity = velocity
        self.safety_margin = safety_margin
        self.weight_limit = weight_limit
        self.current_location = initial_location
        self.other_crane = other_crane
        self.piles = piles
        self.conveyors = conveyors
        self.monitor = monitor

        self.target_location = (-1.0, -1.0)
        self.safety_xcoord = -1.0

        self.offset_pile = None
        self.from_piles = []
        self.to_piles = []
        self.plates = []

        self.idle = True
        self.job_type = None
        self.status = "idle"
        self.unloading = False

        self.event_sequencing = None
        self.event_loading = None
        self.event_prioritizing = None

        self.waiting_time = 0.0
        self.empty_travel_time = 0.0
        self.avoiding_time = 0.0

    def run(self):
        while True:
            self.monitor.queue_sequencing[self.id] = self
            self.event_sequencing = self.env.event()
            target_location_id, target_location_code = yield self.event_sequencing

            if target_location_code == "input_point":
                self.job_type = "storage"
                self.offset_pile = self.piles[target_location_id]
                self.monitor.queue_loading[self.id] = self
                self.event_loading = self.env.event()
                loading_pile_ids = yield self.event_loading

                for pile_id in loading_pile_ids:
                    self.from_piles.append(pile_id)

            elif target_location_code == "pile":
                self.job_type = "reshuffle"
                self.offset_pile = self.piles[target_location_id]
                self.monitor.queue_loading[self.id] = self
                self.event_loading = self.env.event()
                loading_pile_ids = yield self.event_loading

                for pile_id in loading_pile_ids:
                    self.from_piles.append(pile_id)

            elif target_location_code == "conveyor":
                self.job_type = "retrieval"

            self.status = "loading"
            yield self.env.process(self.move())

    def move(self):
        if self.status == "loading":
            location_list = self.from_piles[:]
        elif self.status == "unloading":
            location_list = self.to_piles[:]
        else:
            print("Invalid Access")
            return

        for location in location_list:













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





