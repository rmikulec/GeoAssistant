class AnalysisSQLStepFailed(Exception):
    def __init__(self, analysis_name:str, step: "GISAnalysisStep", exception: Exception):
        self.message = (
            f"SQL Step failed for Analysis {analysis_name}: {step.name}"
            f"  Generate SQL failed to run based on params: \n {step.model_dump_json(indent=2)}"
            f"  Exception raised: {exception}"
        )