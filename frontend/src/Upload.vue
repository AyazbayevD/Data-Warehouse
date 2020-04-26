<style scoped>


  html, body, #upload{
    height: 100%;
    width: 100%;
    background: #f8f9fa;
  }

  #upload {
    font-family: 'Avenir', Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: center;
    display: table;
  }

  #inner_remaining{
    display: table-row;
    background: #f8f9fa;
  }

  input[type="file"]{
    position: relative;
    margin-top: 10px;
    display: none;
  }

  div.file-listing{
    width: 200px;
  }

  span.remove-file{
    color: red;
    cursor: pointer;
    float: right;
  }


  div.scroll{
    max-height: 450px;
    height: 450px;
    overflow-y: scroll;
    margin: 20px 100px;
    background: #f5f5f5;
    border-radius: 10px;
  }

  div.prog_bar{
    margin-left: 100px;
    margin-right: 100px;
  }




</style>

<template>

  <div id="upload">

    <div id="inner_remaining">

      <label>
        <form>
          <div class="form-group">
            <input type="file" id="files" ref="files" class="form-control-file" multiple v-on:change="handleFilesUpload()"/>
          </div>
        </form>
      </label>

      <div class="scroll overflow-auto">
        <ul class="list-group">
          <li v-for="(file, key) in files" class="list-group-item">{{ file.name }} <span class="remove-file" v-on:click="removeFile( key )">Remove</span></li>
        </ul>
      </div>

      <div class="prog_bar">
        <b-progress max="100" :value="uploadPercentage" class="mb-3"></b-progress>
      </div>

      <br>
      <br>

      <button type="button" class="btn btn-outline-secondary" v-on:click="addFiles()">Add Files</button>

      <button type="button" class="btn btn-outline-secondary" v-on:click="submitFiles()">Submit it</button>

    </div>

  </div>

</template>


<script>
  export default {
    /*
      Defines the data used by the component
    */
    data(){
      return {
        files: [],
        uploadPercentage: 0
      }
    },

    /*
      Defines the method used by the component
    */
    methods: {
      /*
        Adds a file
      */
      addFiles(){
        this.$refs.files.click();
      },

      /*
        Submits files to the server
      */
      submitFiles(){
        /*
          Initialize the form data
        */
        let formData = new FormData();

        /*
          Iteate over any file sent over appending the files
          to the form data.
        */
        for( var i = 0; i < this.files.length; i++ ){
          let file = this.files[i];

          formData.append('files[' + i + ']', file);
        }

        /*
          Make the request to the POST /select-files URL
        */
        axios.post( 'http://192.168.1.202:8081/files/file',
          formData,
          {
            headers: {
                'Content-Type': 'multipart/form-data'
            },
            onUploadProgress: function( progressEvent ) {
              this.uploadPercentage = parseInt(Math.round( ( progressEvent.loaded / progressEvent.total ) * 100))
            }.bind(this)

          }

        ).then(response => {
          console.log(response)
          //window.location.reload()
        })
        .catch(error => {
          console.log(error.response)
          //window.location.reload()
        });


      },

      /*
        Handles the uploading of files
      */
      handleFilesUpload(){
        let uploadedFiles = this.$refs.files.files;


        /*
          Adds the uploaded file to the files array
        */
        for( var i = 0; i < uploadedFiles.length; i++ ){
          this.files.push( uploadedFiles[i] );
        }

      },

      /*
        Removes a select file the user has uploaded
      */
      removeFile( key ){
        this.files.splice( key, 1 );
      }
    }
  }
</script>
