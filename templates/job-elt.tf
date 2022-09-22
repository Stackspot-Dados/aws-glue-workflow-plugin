resource "aws_glue_job" "{{inputs.job_name}}" { 
    name = "{{inputs.job_name}}" 
    role_arn = "{{inputs.role_arn}}"
    
    execution_property { 
        max_concurrent_runs = 10 
    }

    command { 
        name = "pythonshell" 
        script_location = "s3://{{inputs.bucket_name}}/automation/script.py" 
        python_version = "3" 
    } 
        
    max_retries = 0 
    timeout = 2880 
    glue_version = "1.0" 
    max_capacity = 0.0625 

    default_arguments = { 
        "--DB_CRAWLER_RELATION" = "{db_name: crawler_name}" # substituir suas variáveis aqui
    }

}

resource "aws_glue_workflow" "workflow-pipeline-dados" {
    name = "workflow-pipeline-dados"
    max_concurrent_runs = 10
}

resource "aws_glue_trigger" "start-trigger" {
    name = "start-trigger"
    type = "ON_DEMAND"
    workflow_name = aws_glue_workflow.workflow-pipeline-dados

    actions {
      job_name = "{{inputs.job_name}}"
    }
}

resource "aws_s3_object" "elt-script-python" {
  bucket = "{{inputs.bucket_name}}"
  key = "automation/script.py"
  source = "./script.py"
}