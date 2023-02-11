import sys
from spark.model_trainer.trainer import ModelTrainer


def run_service(pretrained_file: str = ''):
    if pretrained_file:
        trainer = ModelTrainer(pretrained_file)
    else:
        trainer = ModelTrainer()

    trainer.run()


if len(sys.argv) > 1:
    run_service(sys.argv[1])
else:
    run_service()
