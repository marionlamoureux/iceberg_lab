 Creating an environment file for a project means that users can create a template with variables that could be used to
store environment-specific configuration.
For example, you might have a development, staging and production environment, each containing different clusters,
databases, service URLs and authentication methods. Projects and environments allow you to write the logic and create the resources once, and use template placeholders for values that need to be replaced with the environment
specific parameters.
To each project, you can create multiple environments, but only one can be active at a time for a project.
Environments can be exported to files, and can be imported again to be used for another project, or on another cluster.
While environments are applied to a given project, they are not part of the project. They are not synchronized to Git
when exporting or importing the project. This separation is what allows the storing of environment-specific values, or
configurations that you do not want to expose to the Git repository.