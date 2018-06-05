# 2018-JAVA-Cesco
Detecting fraud clicks using machine learning 

## execution script
### Amazon Linux
```bash
# update
sudo yum update -y

# install git
sudo yum install git -y

# install maven and java 1.8
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven java-1.8.0-openjdk-devel.x86_64
sudo yum remove -y java-1.7.0*

mvn --version

# clone repo
git clone https://github.com/Java-Cesco/Detecting_fraud_clicks.git
cd Detecting_fraud_clicks

# maven build
mvn package

# run
java -jar target/assembly/Detecting_fraud_clicks-aggregation.jar train_sample.csv agg_data
java -jar target/assembly/Detecting_fraud_clicks-decisionTree.jar agg_data

```