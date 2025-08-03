# sDAG

sDag is a micro-orchestration framework for organizing functions in a directed acyclic graph.

Features:

- Conditional branching by using a "Branch" task-type
- Error handling and task clean-up using "on_success" and "on_error" callbacks
- True parallelism by leveraging pathos
- A flexible API for building DAGs using method chaining

