library 'stable@develop'
def determineRepoName() {
    return scm.getUserRemoteConfigs()[0].getUrl().tokenize('/').last().split("\\.")[0]
}
pipeline {
    options{
        ansiColor 'xterm'
    }
    agent {
        label 'linux'
    }
    environment{
        REPO_NAME = determineRepoName()
    }
    stages {
        stage('Prepare folders'){
            steps {
                sh 'mkdir -p /home/jenkins/.ivy2 /home/jenkins/.sbt'
            }
        }
        stage('Prepare pipeline') {
            agent {
                dockerfile {
                    reuseNode true
                }
            }
            stages {
                stage('Fetch pipeline tool'){
                    steps {
                        sh 'pip3 install --user --extra-index-url https://nexus.se.telenor.net/repository/pypi-internal/simple --upgrade telenor-airflow-pipeline'
                    }
                }
                stage('Build & Test'){
                    steps {
                        sh 'sbt  test assembly publishLocal'
                    }
                }
                stage('Publish Snapshot'){
                    steps{
                        configFileProvider([configFile(fileId: 'sbt-snapshots.credentials', variable: 'CREDENTIALS_FILE')]) {
                            sh """sbt -batch 'set credentials+=Credentials(file("${CREDENTIALS_FILE}"))' publish"""
                        }
                    }
                }
                stage('Publish Release'){
                    when{
                        branch 'master'
                    }
                    environment{
                        APP_BUILD_VERSION="0.1.${BUILD_NUMBER}"
                    }
                    steps{
                        configFileProvider([configFile(fileId: 'sbt-releases.credentials', variable: 'CREDENTIALS_FILE')]) {
                            sh """sbt -Dapp.build.version=${APP_BUILD_VERSION} -batch 'set credentials+=Credentials(file("${CREDENTIALS_FILE}"))' publish"""
                        }

                    }
                }
                stage("Deploy to airflow prod"){
//                    when{
//                        branch 'master'
//                    }
                    steps {
                        sh "~/.local/bin/pipeline deploy --name $REPO_NAME"
                    }
                }
            }
        }
        stage('Clean') {
            steps {
                cleanWs()
            }
        }
    }
}