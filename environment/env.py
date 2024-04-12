import torch
import simpy
import numpy as np
import pandas as pd

from torch_geometric.data import HeteroData
from environment.data import DataGenerator
from environment.simulation import Crane, InputPoint, Pile, OutputPoint, Plate, Monitor


class SteelStockyard:
    def __init__(self, data_src, config, look_ahead=2):
        self.data_src = data_src
        self.config = config
        self.look_ahead = look_ahead
        self.row_range = config['row_range']
        self.bay_range = config['bay_range']
        self.retrieval_pile_range = config['retrieval_pile_range']
        self.input_point_coords = config['input_point_coords']
        self.output_point_coords = config['output_point_coords']
        self.inter_retrieval_times = config['inter_retrieval_times']
        self.crane_velocity = config['crane_velocity']
        self.safety_margin = config['safety_margin']
        self.weight_limit = config['weight_limit']
        self.number_limit = config['number_limit']
        self.pile_limit = config['pile_limit']
        self.crane_1_initial_coord = config['crane_1_initial_coord']
        self.crane_2_initial_coord = config['crane_2_initial_coord']

        if type(self.data_src) is DataGenerator:
            self.df_storage, self.df_reshuffle, self.df_retrieval = self.data_src.generate()
        else:
            self.df_storage = pd.read_excel(self.data_src, sheet_name="storage", engine="openpyxl")
            self.df_reshuffle = pd.read_excel(self.data_src, sheet_name="reshuffle", engine="openpyxl")
            self.df_retrieval = pd.read_excel(self.data_src, sheet_name="retrieval", engine="openpyxl")

        self.decision_mode = {0: "sequencing", 1: "multi-loading", 2: "prioritizing"}

        self.action_size = len(self.pile_list) * len(self.crane_list)
        self.state_size = {"crane": 2, "pile": 1 + 1 * look_ahead}
        self.meta_data = (["crane", "pile"],
                          [("pile", "moving_rev", "crane"),
                           ("crane", "moving", "pile")])
        self.num_nodes = {"crane": len(self.crane_list), "pile": len(self.pile_list)}


    def step(self, action, mode):
        if self.decision_mode[mode] == "sequencing":
            self._step_for_sequencing(action)
        elif self.decision_mode[mode] == "prioritizing":
            self._step_for_prioritizing(action)
        else:
            print("Wrong decision mode")

        while True:
            while True:
                if self.monitor.scheduling_for_sequencing:
                    mode = 0
                    break
                elif self.monitor.scheduling_for_prioritizing:
                    mode = 1
                    break
                else:
                    if len(self.model.move_list) == 0 and all([crane.idle for crane in self.cranes]):
                        mode = 0
                        done = True
                        break
                    elif len(self.model.move_list) == 0 and not all([crane.idle for crane in self.cranes]):
                        mode = 1
                        done = True
                        break
                    else:
                        self.env.step()


        return next_state, reward, done, mode

    def _step_for_sequencing(self, action):
        crane_id = action % self.num_nodes["crane"]
        pile_id = action // self.num_nodes["crane"]
        pile_name = self.action_mapping[pile_id]
        done = False

        self.cranes[crane_id].event.succed(pile_name)

    def reset(self):
        self.env, self.input_points, self.output_points, self.piles, self.cranes, self.monitor \
            = self._build_simulation_model()

        while True:
            while True:
                flag, info = self.monitor.request_scheduling()
                if flag:
                    while self.env.now in [event[0] for event in self.env._queue]:
                        self.env.step()
                    break
                else:
                    self.env.step()

            initial_state, mask = self._get_state(info)

            if info == "sequencing" and not mask.any():
                if not mask[0].any():
                    self.cranes[0].event_sequencing.succeed(("None", "None"))
                if not mask[1].any():
                    self.cranes[1].event_sequencing.succeed(("None", "None"))
                break
            else:
                break

            return initial_state, mask


    def _get_state(self, info):
        pass


    def _build_simulation_model(self):
        env = simpy.Environment()
        monitor = Monitor()

        row_list = [chr(i) for i in range(ord(self.row_range[0]), ord(self.row_range[1]) + 1)]
        bay_list = [i for i in range(self.bay_range[0], self.bay_range[1] + 1)]

        input_points = {}
        output_points = {}
        piles = {}

        location_id = 0
        for i, row_id in enumerate(row_list):
            for j, bay_id in enumerate(bay_list):
                if bay_id in self.input_point_coords:
                    name = "I" + str(bay_id).rjust(2, '0')
                    coord = (bay_id,)
                    plates = []
                    df_plates = self.df_storage[self.df_storage["from_location"] == name]
                    for k, row in df_plates.iterrows():
                        plate = Plate(row["name"], row["id"], row["from_location"], row["to_location"], row["weight"])
                        plates.append(plate)
                    input_points[location_id] = InputPoint(env, name, location_id, coord, plates, monitor)
                elif bay_id in self.output_point_coords:
                    name = "O" + str(bay_id).rjust(2, '0')
                    coord = (bay_id,)
                    irt = self.inter_retrieval_times[bay_id]
                    output_points[location_id] = OutputPoint(env, name, location_id, coord, irt, monitor)
                else:
                    name = row_id + str(bay_id).rjust(2, '0')
                    type = "retrieval" if self.retrieval_pile_range[0] <= bay_id <= self.retrieval_pile_range[1] \
                        else "storage"
                    coord = (i, bay_id)
                    plates = []
                    if type == "storage":
                        df_plates = self.df_reshuffle[self.df_reshuffle["from_location"] == name]
                    else:
                        df_plates = self.df_retrieval[self.df_retrieval["from_location"] == name]
                    for k, row in df_plates.iterrows():
                        plate = Plate(row["name"], row["id"], row["from_location"], row["to_location"], row["weight"])
                        plates.append(plate)
                    piles[location_id] = Pile(env, name, location_id, type, coord, plates, monitor)
                location_id += 1

        cranes = {}
        crane1 = Crane(env, 'Crane-1', 0, self.crane_velocity, self.safety_margin, self.crane_1_initial_coord,
                       input_points, piles, output_points, monitor, self.row_range, self.bay_range)
        crane2 = Crane(env, 'Crane-2', 1, self.crane_velocity, self.safety_margin, self.crane_2_initial_coord,
                       input_points, piles, output_points, monitor, self.row_range, self.bay_range)

        crane1.other_crane = crane2
        crane2.other_crane = crane1

        cranes[0] = crane1
        cranes[1] = crane2

        return env, input_points, output_points, piles, cranes, monitor


if __name__ == "__main__":
    import json
    config = {
        "row_range": (0, 1),
        "bay_range": (0, 43),
        "retrieval_pile_range": (16, 33),
        "input_point_coords": (0,), 
        "output_point_coords": (22, 26, 43),
        "inter_retrieval_times": {22: 0.01, 26: 0.01, 43: 0.005},
        "num_to_piles_for_storage": (5,),
        "num_from_piles_for_reshuffle": 10,
        "num_to_piles_for_reshuffle": 10,
        "num_from_piles_for_retrieval": {22: 5, 26: 5, 43: 2},
        "num_plates_for_storage": 150,
        "num_plates_for_reshuffle": 150,
        "num_plates_for_retrieval": 150,
        "crane_velocity": (0.5, 1.0),
        "weight_limit": 20.0,
        "number_limit": 3,
        "pile_limit": 2,
        "crane_1_initial_coord": (1, 0),
        "crane_2_initial_coord": (42, 0),
        "safety_margin": 5
    }

    file_path = "../input/env_config.json"
    with open(file_path, 'w') as f:
        json.dump(config, f, indent=4)

