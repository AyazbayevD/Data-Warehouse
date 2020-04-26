import Vue from 'vue'
import Router from 'vue-router'
import Upload from '../Upload'
import Graph from '../Graph'
import Home from '../Home'
import Person from "../Person";

Vue.use(Router)


export default new Router({
  routes: [
    {
      path: '/uploadFiles',
      name: 'uploadFiles',
      component: Upload
    },

    {
      path: '/graph',
      name: 'Graph',
      component: Graph
    },

    {
      path: '/',
      redirect: {
        name: 'Graph'
      }
    },

    {
      path: '/person',
      name: 'Person',
      component: Person,
      props: true
    }

  ]
})
