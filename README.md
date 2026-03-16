# AWS Samples

Sample code and POCs 

#### NOTE: the workarea directories can be used to store temporary files which will not be commited to repo

### Source control 

- Use docker image 
```bash
cd $HOME/CODE/personal/aws_samples
docker run -it --rm -v $(pwd):/code -v ~/.gitconfig:/root/.gitconfig -v ~/.ssh:/root/.ssh:ro lazygit-container
```