import torch
import numpy as np
import pandas as pd

from torch_geometric.data import HeteroData
from environment.data import DataGenerator
from environment.simulation import Management


class SteelStockyard:
    def __init__(self, data_src, look_ahead=2,
                 row_range=(0, 1), bay_range=(0, 43), input_points=(0,), output_points=(22, 26, 43)):
        self.data_src = data_src
        self.look_ahead = look_ahead
        self.row_range = row_range
        self.bay_range = bay_range
        self.input_points = input_points
        self.output_points = output_points

        if type(self.data_src) is DataGenerator:
            self.df_storage, self.df_reshuffle, self.df_retrieval = self.data_src.generate()
        else:
            self.df_storage = pd.read_excel(self.data_src, sheet_name="storage", engine="openpyxl")
            self.df_reshuffle = pd.read_excel(self.data_src, sheet_name="reshuffle", engine="openpyxl")
            self.df_retrieval = pd.read_excel(self.data_src, sheet_name="retrieval", engine="openpyxl")

        self.decision_mode = {0: "sequencing", 1: "prioritizing"}

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



    def _step_for_prioritizing(self, action):
