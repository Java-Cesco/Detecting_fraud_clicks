# 2018-JAVA-Cesco
Detecting fraud clicks using machine learning 

## execution script
### Amazon Linux
```bash
# update
sudo yum update -y

# install git
sudo yum install git -y

# install maven
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn --version


```