from prefect.filesystems import GitHub

gh = GitHub(
    repository="https://github.com/ericyaang/super-pipe", reference="main"
)
gh.save("super-pipe", overwrite=True)