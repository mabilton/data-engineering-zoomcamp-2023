# Week 2 Homework

This directory contains all of my Week 2 Homework files; this week's homework focuses on using Prefect to orchestrate data engineering workflows. More specifically: 
- `01-questions.md` contains all of the Week 2 homework questions without corresponding answers.
- `02-answers.md` contains the answer to each question listed in `01-questions.md`.
- `requirements.txt` lists all of the Python modules that must be installed to run the code in this subdirectory; these requirements can be installed by calling `pip install -r requirements.txt`.

The code used to answer the homework questions is organised into four subdirectories:
- The `terraform` subdirectory contains Terraform code that deploys the GCP resources that we interact with in `02-answers.md`. These files are essentially identical to what is found in the `01-intro/homework/terraform` directory.
The `bash` commands used to run . are given in `02-answers.md`.
- The `flows` subdirectory contains the Python code for the Prefect flows we run in to answer the homework questions.
- The `blocks` subdirectory contains all the Python code used to create the Prefect blocks utilised by the flows defined in the `flows` directory; although these blocks can be created using the Prefect Orion UI, we choose to create these blocks using code for ease of reproducability.

Finally, we note that the `screenshots` subdirectory, unsurprisingly, contains screenshots that are used to complement the answers described in `02-answers.md`. 
