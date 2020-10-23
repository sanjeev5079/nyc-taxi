library 'stable'

pipeline {
  agent {
      docker {
          label 'linux'
          image 'maven:3.6.3-jdk-11'
          args containerSettings()
          reuseNode true
      }
  }
  options {
      timestamps()
      buildDiscarder(logRotator(numToKeepStr: '5'))
      timeout(time: 1, unit: 'HOURS')
  }
  stages {
    stage('Build and Test') {
       steps {
         withMaven {
            sh 'mvn clean install'
            junit '**/surefire-reports/*.xml'
         }
       }
    }
    stage('Publish Artifacts') {
      when {
        anyOf {
              branch 'master'
              branch 'develop'
            }
        }
        steps {
          withMaven {
            sh 'mvn deploy -DskipTests'
          }
        }
    }
    stage('Continuous Static Analysis') {
      when {
        anyOf {
          branch 'master'
          branch 'develop'
        }
      }
      steps {
        runPersistedSonarAnalysis()
      }
    }
    stage('SonarQube Reviewer') {
      when {
        anyOf {
          changeRequest()
        }
      }
      steps {
        pullRequestStaticAnalysisChecks()
      }
    }
  }
  post {
    changed {
        email isMainlineBranch() ? leadDeveloper() : nobody()
    }
    regression {
        email isChangeRequest() ? committers() : nobody()
    }
    success {
        email isChangeRequest() ? committers() : nobody()
    }
  }
}
