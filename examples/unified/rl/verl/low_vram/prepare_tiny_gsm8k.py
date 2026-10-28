#!/usr/bin/env python3
# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0.

import argparse
from pathlib import Path

import pandas as pd


TRAIN_EXAMPLES = [
    ("Janet has 3 apples and buys 5 more. How many apples does she have?", "8"),
    ("A box has 12 pens. Tom takes 4. How many pens remain?", "8"),
    ("There are 6 bags with 2 marbles each. How many marbles are there?", "12"),
    ("Lily reads 7 pages on Monday and 9 on Tuesday. How many pages?", "16"),
    ("A ticket costs 3 dollars. How much do 4 tickets cost?", "12"),
    ("Mia had 10 stickers and gave away 6. How many stickers are left?", "4"),
    ("Sam runs 5 miles each day for 3 days. How many miles total?", "15"),
    ("A shelf has 18 books split equally into 3 stacks. Books per stack?", "6"),
    ("Nora bakes 2 trays with 8 cookies each. How many cookies?", "16"),
    ("A train travels 20 miles, then 15 more. How far did it travel?", "35"),
    ("Ben has 9 cards and gets 11 more. How many cards now?", "20"),
    ("A class has 24 students in 4 equal teams. Students per team?", "6"),
    ("Kim buys 5 packs with 3 pencils each. How many pencils?", "15"),
    ("There are 30 oranges in 5 boxes equally. Oranges per box?", "6"),
    ("A movie starts at 2 and lasts 3 hours. What hour does it end?", "5"),
    ("A rope is 14 meters and is cut by 5 meters. How much remains?", "9"),
]

TEST_EXAMPLES = [
    ("Alex has 4 coins and finds 7 more. How many coins?", "11"),
    ("Two baskets hold 6 eggs each. How many eggs total?", "12"),
    ("Sara has 15 candies and eats 5. How many candies remain?", "10"),
    ("A car uses 2 liters per trip for 4 trips. How many liters?", "8"),
]


def build_rows(examples, split):
    rows = []
    for idx, (question, answer) in enumerate(examples):
        rows.append(
            {
                "data_source": "tiny_gsm8k",
                "prompt": [
                    {
                        "role": "user",
                        "content": (
                            "Solve the math problem. Give the final answer "
                            f"after ####.\n\n{question}"
                        ),
                    }
                ],
                "ability": "math",
                "reward_model": {"style": "rule", "ground_truth": answer},
                "extra_info": {"split": split, "index": idx, "answer": answer},
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="/data/gsm8k")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(build_rows(TRAIN_EXAMPLES, "train")).to_parquet(
        output_dir / "train.parquet"
    )
    pd.DataFrame(build_rows(TEST_EXAMPLES, "test")).to_parquet(
        output_dir / "test.parquet"
    )
    print(f"Wrote tiny GSM8K-style parquet files to {output_dir}")


if __name__ == "__main__":
    main()
