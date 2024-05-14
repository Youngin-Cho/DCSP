import os
import random
import numpy as np
import pandas as pd


class DataGenerator:
    def __init__(self, config):
        self.num_to_piles_for_storage = config['num_to_piles_for_storage']
        self.num_from_piles_for_reshuffle = config['num_from_piles_for_reshuffle']
        self.num_to_piles_for_reshuffle = config['num_to_piles_for_reshuffle']
        self.num_from_piles_for_retrieval = config['num_from_piles_for_retrieval']
        self.num_plates_for_storage = config['num_plates_for_storage']
        self.num_plates_for_reshuffle = config['num_plates_for_reshuffle']
        self.num_plates_for_retrieval = config['num_plates_for_retrieval']
        self.safety_margin = config['safety_margin']
        self.storage_piles, self.retrieval_piles = self.read_config(config)

    def read_config(self, config):
        row_range = config['row_range']
        bay_range = config['bay_range']
        input_point_coords = config['input_point_coords']
        output_point_coords = config['output_point_coords']
        storage_pile_range = config['storage_pile_range']
        retrieval_pile_range = config['retrieval_pile_range']

        storage_piles = {key:{} for key in input_point_coords}
        retrieval_piles = {key:{} for key in output_point_coords}
        for row_id in range(row_range[0], row_range[0] + 1):
            for col_id in range(bay_range[0], bay_range[1] + 1):
                if (not col_id in input_point_coords) and (not col_id in output_point_coords):
                    flag = True
                    for key in retrieval_pile_range.keys():
                        min_col, max_col = retrieval_pile_range[key]
                        if min_col <= col_id <= max_col:
                            retrieval_piles[int(key)][col_id] = "%s%d" % (chr(row_id + 65), col_id)
                            flag = False
                    if flag:
                        for key in storage_pile_range.keys():
                            min_col, max_col = storage_pile_range[key]
                            if min_col <= col_id <= max_col:
                                storage_piles[int(key)][col_id] = "%s%d" % (chr(row_id + 65), col_id)

        return storage_piles, retrieval_piles

    def generate(self, file_path=None):
        df_storage = pd.DataFrame(columns=["pileno", "pileseq", "markno", "unitw", "topile"])
        df_reshuffle = pd.DataFrame(columns=["pileno", "pileseq", "markno", "unitw", "topile"])
        df_retrieval = pd.DataFrame(columns=["pileno", "pileseq", "markno", "unitw", "topile"])

        if self.num_plates_for_retrieval > 0:
            candidates = piles_in_area2 + piles_in_area3 + piles_in_area4
            # candidates = [i for i in candidates if not i in ["A22", "A23", "A24", "B22", "B23", "B24"]]
            from_piles_retrieval_cn1 = random.sample(candidates, self.n_from_piles_retrieval_cn1)
            candidates = [i for i in candidates if not i in from_piles_retrieval_cn1]
            from_piles_retrieval_cn2 = random.sample(candidates, self.n_from_piles_retrieval_cn2)
            if "Crane-2" in self.working_crane_ids:
                candidates = piles_in_area6
                from_piles_retrieval_cn3 = random.sample(candidates, self.n_from_piles_retrieval_cn3)
            else:
                from_piles_retrieval_cn3 = []
            from_piles_retrieval = from_piles_retrieval_cn1 + from_piles_retrieval_cn2 + from_piles_retrieval_cn3
            for pile in from_piles_retrieval:
                num_of_plates = random.randint(int(0.9 * self.n_plates_retrieval), int(1.1 * self.n_plates_retrieval))
                pileno = [pile] * num_of_plates
                pileseq = [str(i).rjust(3, '0') for i in range(1, num_of_plates + 1)]
                markno = ["SP-RT-%s-%s" % (pile, i) for i in pileseq]
                unitw = np.random.uniform(0.141, 19.294, num_of_plates)
                if pile in from_piles_retrieval_cn1:
                    topile = ["cn1"] * num_of_plates
                elif pile in from_piles_retrieval_cn2:
                    topile = ["cn2"] * num_of_plates
                else:
                    topile = ["cn3"] * num_of_plates
                df_temp = pd.DataFrame(
                    {"pileno": pileno, "pileseq": pileseq, "markno": markno, "unitw": unitw, "topile": topile})
                df_retrieval = pd.concat([df_retrieval, df_temp], ignore_index=True)
        else:
            from_piles_retrieval = []

    def generate_pre(self, file_path=None):
        # 입고, 선별, 출고 데이터를 저장하기 위한 데이터프레임 생성
        df_storage = pd.DataFrame(columns=["pileno", "pileseq", "markno", "unitw", "topile"])
        df_reshuffle = pd.DataFrame(columns=["pileno", "pileseq", "markno", "unitw", "topile"])
        df_retrieval = pd.DataFrame(columns=["pileno", "pileseq", "markno", "unitw", "topile"])

        # 강재 적치장 내 모든 파일이 포함된 리스트 생성
        mapping_from_pile_to_x = {}

        piles_in_area0 = []
        for row_id in self.rows:
            pile = row_id + "00"
            piles_in_area0.append(pile)
            mapping_from_pile_to_x[pile] = 1

        piles_in_area1, piles_in_area2, piles_in_area3, piles_in_area4, piles_in_area5, piles_in_area6 = [], [], [], [], [], []
        n_bays_cum = np.cumsum([self.n_bays_in_area1, self.n_bays_in_area2, self.n_bays_in_area3,
                                self.n_bays_in_area4, self.n_bays_in_area5, self.n_bays_in_area6])

        for row_id in self.rows:
            for col_id in range(1, n_bays_cum[-1] + 1):
                pile = row_id + str(col_id).rjust(2, '0')
                if col_id <= n_bays_cum[0]:
                    piles_in_area1.append(pile)
                    mapping_from_pile_to_x[pile] = col_id + 1
                elif n_bays_cum[0] < col_id <= n_bays_cum[1]:
                    piles_in_area2.append(pile)
                    mapping_from_pile_to_x[pile] = col_id + 1
                elif n_bays_cum[1] < col_id <= n_bays_cum[2]:
                    piles_in_area3.append(pile)
                    mapping_from_pile_to_x[pile] = col_id + 2
                elif n_bays_cum[2] < col_id <= n_bays_cum[3]:
                    piles_in_area4.append(pile)
                    mapping_from_pile_to_x[pile] = col_id + 3
                elif n_bays_cum[3] < col_id <= n_bays_cum[4]:
                    piles_in_area5.append(pile)
                    mapping_from_pile_to_x[pile] = col_id + 3
                else:
                    piles_in_area6.append(pile)
                    mapping_from_pile_to_x[pile] = col_id + 3

        piles_all = piles_in_area0 + piles_in_area1 + piles_in_area2 \
                    + piles_in_area3 + piles_in_area4 + piles_in_area5 + piles_in_area6
        x_max = max(mapping_from_pile_to_x.values()) + 1

        # 출고 계획 생성
        if self.retrieval:
            candidates = piles_in_area2 + piles_in_area3 + piles_in_area4
            # candidates = [i for i in candidates if not i in ["A22", "A23", "A24", "B22", "B23", "B24"]]
            from_piles_retrieval_cn1 = random.sample(candidates, self.n_from_piles_retrieval_cn1)
            candidates = [i for i in candidates if not i in from_piles_retrieval_cn1]
            from_piles_retrieval_cn2 = random.sample(candidates, self.n_from_piles_retrieval_cn2)
            if "Crane-2" in self.working_crane_ids:
                candidates = piles_in_area6
                from_piles_retrieval_cn3 = random.sample(candidates, self.n_from_piles_retrieval_cn3)
            else:
                from_piles_retrieval_cn3 = []
            from_piles_retrieval = from_piles_retrieval_cn1 + from_piles_retrieval_cn2 + from_piles_retrieval_cn3
            for pile in from_piles_retrieval:
                num_of_plates = random.randint(int(0.9 * self.n_plates_retrieval), int(1.1 * self.n_plates_retrieval))
                pileno = [pile] * num_of_plates
                pileseq = [str(i).rjust(3, '0') for i in range(1, num_of_plates + 1)]
                markno = ["SP-RT-%s-%s" % (pile, i) for i in pileseq]
                unitw = np.random.uniform(0.141, 19.294, num_of_plates)
                if pile in from_piles_retrieval_cn1:
                    topile = ["cn1"] * num_of_plates
                elif pile in from_piles_retrieval_cn2:
                    topile = ["cn2"] * num_of_plates
                else:
                    topile = ["cn3"] * num_of_plates
                df_temp = pd.DataFrame(
                    {"pileno": pileno, "pileseq": pileseq, "markno": markno, "unitw": unitw, "topile": topile})
                df_retrieval = pd.concat([df_retrieval, df_temp], ignore_index=True)
        else:
            from_piles_retrieval = []

        # 선별 계획 생성
        if self.reshuffle:
            candidates = piles_in_area1 + piles_in_area5
            if not "Crane-1" in self.working_crane_ids:
                candidates = [i for i in candidates if mapping_from_pile_to_x[i] >= 1 + self.safety_margin]
            if not "Crane-2" in self.working_crane_ids:
                candidates = [i for i in candidates if mapping_from_pile_to_x[i] <= x_max - self.safety_margin]
            from_piles_reshuffle = random.sample(candidates, self.n_from_piles_reshuffle)
            candidates = [i for i in piles_all if (i not in from_piles_retrieval) and (i not in piles_in_area0)]
            candidates = [i for i in candidates if i not in from_piles_reshuffle]
            if not "Crane-1" in self.working_crane_ids:
                candidates = [i for i in candidates if mapping_from_pile_to_x[i] >= 1 + self.safety_margin]
            if not "Crane-2" in self.working_crane_ids:
                candidates = [i for i in candidates if mapping_from_pile_to_x[i] <= x_max - self.safety_margin]
            to_piles_reshuffle = random.sample(candidates, self.n_to_piles_reshuffle)
            # to_piles_reshuffle = ["A22", "A23", "A24", "B22", "B23", "B24"]

            for pile in from_piles_reshuffle:
                x = mapping_from_pile_to_x[pile]
                if x < 1 + self.safety_margin:
                    to_piles_reshuffle_rev = [i for i in to_piles_reshuffle
                                              if mapping_from_pile_to_x[i] <= x_max - self.safety_margin]
                elif x > x_max - self.safety_margin:
                    to_piles_reshuffle_rev = [i for i in to_piles_reshuffle
                                              if mapping_from_pile_to_x[i] >= 1 + self.safety_margin]
                else:
                    to_piles_reshuffle_rev = to_piles_reshuffle
                num_of_plates = random.randint(int(0.9 * self.n_plates_reshuffle), int(1.1 * self.n_plates_reshuffle))
                pileno = [pile] * num_of_plates
                pileseq = [str(i).rjust(3, '0') for i in range(1, num_of_plates + 1)]
                markno = ["SP-RS-%s-%s" % (pile, i) for i in pileseq]
                unitw = np.random.uniform(0.141, 19.294, num_of_plates)
                topile = random.choices(to_piles_reshuffle_rev, k=num_of_plates)
                df_temp = pd.DataFrame(
                    {"pileno": pileno, "pileseq": pileseq, "markno": markno, "unitw": unitw, "topile": topile})
                df_reshuffle = pd.concat([df_reshuffle, df_temp], ignore_index=True)
        else:
            from_piles_reshuffle = []

        # 적치 계획 생성
        if self.storage:
            if "Crane-1" in self.working_crane_ids:
                from_piles_storage = random.sample(piles_in_area0, self.n_from_piles_storage)
            else:
                from_piles_storage = []
            candidates = piles_in_area1 + piles_in_area5
            candidates = [i for i in candidates if i not in from_piles_reshuffle]
            candidates = [i for i in candidates if mapping_from_pile_to_x[i] <= x_max - self.safety_margin]
            to_piles_storage = random.sample(candidates, self.n_to_piles_storage)
            # to_piles_storage = ["A22", "A23", "A24", "B22", "B23", "B24"]

            for pile in from_piles_storage:
                num_of_plates = random.randint(int(0.9 * self.n_plates_storage), int(1.1 * self.n_plates_storage))
                pileno = [pile] * num_of_plates
                pileseq = [str(i).rjust(3, '0') for i in range(1, num_of_plates + 1)]
                markno = ["SP-ST-%s-%s" % (pile, i) for i in pileseq]
                unitw = np.random.uniform(0.141, 19.294, num_of_plates)
                topile = random.choices(to_piles_storage, k=num_of_plates)
                df_temp = pd.DataFrame({"pileno": pileno, "pileseq": pileseq, "markno": markno, "unitw": unitw, "topile": topile})
                df_storage = pd.concat([df_storage, df_temp], ignore_index=True)

        if file_path is not None:
            writer = pd.ExcelWriter(file_path)
            df_storage.to_excel(writer, sheet_name="storage", index=False)
            df_reshuffle.to_excel(writer, sheet_name="reshuffle", index=False)
            df_retrieval.to_excel(writer, sheet_name="retrieval", index=False)
            writer.save()

        return df_storage, df_reshuffle, df_retrieval


if __name__ == '__main__':
    import json
    config_path = "../input/env_config.json"
    with open(config_path, 'r') as f:
        config = json.load(f)

    data_src = DataGenerator(config)