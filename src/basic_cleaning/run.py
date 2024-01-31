#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Fetch the artifact from W&B
    logger.info("Download input artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Read the artifact
    df = pd.read_csv(artifact_local_path)
    
    # Drop Outliers
    logger.info("Drop Outliers: Keeping the values between the min_price and the max_price")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    #Convert the column last_review to datetime
    logger.info("Convert the column last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save the resulting dataframe into a csv file
    logger.info("Save the resulting dataframe into a csv file")
    df.to_csv("clean_sample.csv", index=False)

    # Upload the clean artifact to W&B
    logger.info("Upload the clean artifact to W&B")
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the cleaned artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the cleaned artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the cleaned artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum Price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum Price to consider",
        required=True
    )


    args = parser.parse_args()

    go(args)
